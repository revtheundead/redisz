const std = @import("std");
const Io = std.Io;

pub const GetError = error{WrongType} || std.mem.Allocator.Error;

// Redis list backing. ArrayList gives O(1) tail-push and O(1) LINDEX, at the cost
// of O(n) head-push. Real Redis uses a quicklist (linked list of listpacks);
// swappable behind this alias when it matters.
pub const List = std.ArrayListUnmanaged([]const u8);

pub const StreamEntryId = struct {
    ms: u64,
    seq: u64,

    pub fn order(a: StreamEntryId, b: StreamEntryId) std.math.Order {
        return switch (std.math.order(a.ms, b.ms)) {
            .eq => std.math.order(a.seq, b.seq),
            else => |o| o,
        };
    }
};

// What the caller *asked for*. The store resolves this into a concrete
// StreamEntryId, filling in whichever parts were auto (`*`).
pub const StreamIdSpec = union(enum) {
    explicit: StreamEntryId, // "ms-seq"
    ms_auto_seq: u64, // "ms-*"
    fully_auto, // "*"
};

pub const StreamAddError = error{ IdEqualOrSmaller, IdZero } || GetError;

pub const IncrError = error{ NotInteger, Overflow } || GetError;

pub const StreamEntry = struct {
    id: StreamEntryId, // gpa owned
    fields: [][]const u8, // flat [k0, v0, k1, v1, ...], each gpa owned
};

pub const Stream = std.ArrayListUnmanaged(StreamEntry);

pub const StreamRangeEntry = struct {
    id: StreamEntryId,
    fields: []const []const u8,
};

// Discriminated payload for a key. Hash and zset will land as their
// Codecrafters sections start.
pub const StoredValue = union(enum) {
    string: []const u8,
    list: List,
    stream: Stream,
};

// Every method assumes the caller holds `mutex`. The connection loop takes it
// around each command, so commands execute one at a time, like the single
// execution thread in real Redis. Only the blocking commands release it, while
// they wait.
pub const Store = struct {
    gpa: std.mem.Allocator,
    map: std.StringArrayHashMapUnmanaged(Entry),
    mutex: Io.Mutex,
    // Per-key FIFO of blocked clients. Empty entries are removed when the last
    // waiter for a key is dequeued (see dequeueWaiter).
    waiters: std.StringHashMapUnmanaged(std.DoublyLinkedList),
    // Monotonic modification stamp. Bumped on every key mutation; a key's
    // Entry records the value at its last write. WATCH/EXEC compare these to
    // detect concurrent modification (optimistic locking).
    mutation_seq: u64 = 0,

    pub const Side = enum { head, tail };

    const Entry = struct {
        value: StoredValue,
        expires_at_ms: ?i64,
        version: u64, // mutation_seq at this key's last write
    };

    // A parked BLPOP client. Lives on the caller's stack, the handler task
    // stays inside listPopBlocking until wake/timeout/cancel, so the frame
    // remains valid the whole time the waiter is queued.
    const Waiter = struct {
        // Element handed over by the signaler; gpa-owned. Null while queued.
        // Set (with mutex held) before signal() so the wake sees it.
        delivered: ?[]const u8 = null,
        // Per-waiter condvar. signal() wakes exactly this waiter, not a random one.
        condition: Io.Condition = .init,
        node: std.DoublyLinkedList.Node = .{},
    };

    pub fn init(gpa: std.mem.Allocator) Store {
        return .{ .gpa = gpa, .map = .empty, .mutex = .init, .waiters = .empty };
    }

    pub fn deinit(self: *Store) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.gpa.free(entry.key_ptr.*);
            freeValue(self.gpa, entry.value_ptr.value);
        }
        self.map.deinit(self.gpa);

        // Waiters map: free duped keys. The DoublyLinkedList entries are
        // Waiter nodes owned by their respective task stacks, no cleanup
        // owed here. In practice this map should be empty at shutdown since
        // all client tasks were awaited.
        var wait_it = self.waiters.iterator();
        while (wait_it.next()) |entry| self.gpa.free(entry.key_ptr.*);
        self.waiters.deinit(self.gpa);
    }

    fn freeValue(gpa: std.mem.Allocator, v: StoredValue) void {
        switch (v) {
            .string => |s| gpa.free(s),
            .list => |list| {
                for (list.items) |elem| gpa.free(elem);
                var mut = list;
                mut.deinit(gpa);
            },
            .stream => |stream| {
                for (stream.items) |entry| {
                    for (entry.fields) |f| gpa.free(f);
                    gpa.free(entry.fields);
                }
                var mut = stream;
                mut.deinit(gpa);
            },
        }
    }

    // Runs lazy expiration and returns a pointer to a live entry, or null
    // when the key is absent or just evicted.
    fn getLiveEntry(self: *Store, key: []const u8, now_ms: i64) ?*Entry {
        const entry_ptr = self.map.getPtr(key) orelse return null;
        if (entry_ptr.expires_at_ms) |deadline| {
            if (now_ms >= deadline) {
                self.removeKey(key);
                return null;
            }
        }
        return entry_ptr;
    }

    // Deletes `key` and frees its storage. No-op if absent. Any *Entry
    // obtained before this call is invalid afterwards (swapRemove moves
    // another entry into the freed slot).
    fn removeKey(self: *Store, key: []const u8) void {
        const kv = self.map.fetchSwapRemove(key) orelse return;
        self.gpa.free(kv.key);
        freeValue(self.gpa, kv.value.value);
    }

    // Next global modification stamp.
    fn nextVersion(self: *Store) u64 {
        self.mutation_seq += 1;
        return self.mutation_seq;
    }

    // Releases the mutex for `ms` so other clients can run, then takes it
    // back. Used by the polling waits in BLPOP (with timeout) and XREAD
    // BLOCK. Always returns with the mutex held, even when canceled.
    pub fn sleepUnlocked(self: *Store, io: Io, ms: u64) Io.Cancelable!void {
        self.mutex.unlock(io);
        defer self.mutex.lockUncancelable(io);
        try io.sleep(.fromMilliseconds(@intCast(ms)), .awake);
    }

    // SET overwrites any prior value regardless of prior type.
    pub fn set(self: *Store, key: []const u8, value: []const u8, expires_at_ms: ?i64) !void {
        const key_copy = try self.gpa.dupe(u8, key);
        errdefer self.gpa.free(key_copy);
        const value_copy = try self.gpa.dupe(u8, value);
        errdefer self.gpa.free(value_copy);

        const gop = try self.map.getOrPut(self.gpa, key_copy);
        if (gop.found_existing) {
            self.gpa.free(key_copy);
            freeValue(self.gpa, gop.value_ptr.value);
        }
        gop.value_ptr.* = .{ .value = .{ .string = value_copy }, .expires_at_ms = expires_at_ms, .version = self.nextVersion() };
    }

    // Returns an arena-owned copy of the string value.
    //   null            → key absent or expired
    //   WrongType       → key holds a non-string value
    pub fn get(self: *Store, out_arena: std.mem.Allocator, key: []const u8, now_ms: i64) GetError!?[]const u8 {
        const entry = self.getLiveEntry(key, now_ms) orelse return null;

        return switch (entry.value) {
            .string => |s| try out_arena.dupe(u8, s),
            else => error.WrongType,
        };
    }

    // INCR/INCRBY/DECR/DECRBY core. Absent key counts as 0. An existing TTL
    // is preserved; only the SET family resets expiry.
    pub fn incrBy(self: *Store, key: []const u8, delta: i64, now_ms: i64) IncrError!i64 {
        const maybe_entry = self.getLiveEntry(key, now_ms);
        var cur: i64 = 0;
        if (maybe_entry) |entry| {
            cur = switch (entry.value) {
                .string => |s| std.fmt.parseInt(i64, s, 10) catch return error.NotInteger,
                else => return error.WrongType,
            };
        }

        const res = std.math.add(i64, cur, delta) catch return error.Overflow;

        // min i64 is 19 digits + sign = 20 bytes
        var buf: [20]u8 = undefined;
        const res_str = std.fmt.bufPrint(&buf, "{d}", .{res}) catch unreachable;
        const value_copy = try self.gpa.dupe(u8, res_str);
        errdefer self.gpa.free(value_copy);

        if (maybe_entry) |entry| {
            self.gpa.free(entry.value.string);
            entry.value = .{ .string = value_copy };
            entry.version = self.nextVersion();
        } else {
            const key_copy = try self.gpa.dupe(u8, key);
            errdefer self.gpa.free(key_copy);
            try self.map.put(self.gpa, key_copy, .{
                .value = .{ .string = value_copy },
                .expires_at_ms = null,
                .version = self.nextVersion(),
            });
        }
        return res;
    }

    // Optimistic-locking stamp for a key.
    //   0        → key absent or expired
    //   non-zero → mutation_seq at the key's last write
    // Note: getLiveEntry may lazily expire the key here, which correctly
    // surfaces as version 0 (differs from any prior non-zero watch).
    pub fn watchVersion(self: *Store, key: []const u8, now_ms: i64) u64 {
        const entry = self.getLiveEntry(key, now_ms) orelse return 0;
        return entry.version;
    }

    // Returns the RESP type tag ("string", "list", ...) or null if the key
    // is absent/expired. Callers translate null to "none" for the TYPE command.
    pub fn getType(self: *Store, key: []const u8, now_ms: i64) ?[]const u8 {
        const entry = self.getLiveEntry(key, now_ms) orelse return null;
        return @tagName(entry.value);
    }

    // Push one or more values to `key` on `side`. Creates an empty list if
    // the key is absent/expired. WrongType if the key holds a non-list value.
    // Returns the new list length.
    pub fn listPush(self: *Store, io: Io, key: []const u8, values: []const []const u8, side: Side, now_ms: i64) GetError!usize {
        var entry_ptr: *Entry = undefined;
        if (self.getLiveEntry(key, now_ms)) |ep| {
            switch (ep.value) {
                .list => entry_ptr = ep,
                else => return error.WrongType,
            }
        } else {
            const key_copy = try self.gpa.dupe(u8, key);
            errdefer self.gpa.free(key_copy);

            const gop = try self.map.getOrPut(self.gpa, key_copy);
            gop.value_ptr.* = .{ .value = .{ .list = .empty }, .expires_at_ms = null, .version = 0 };
            entry_ptr = gop.value_ptr;
        }
        const list_ptr = &entry_ptr.value.list;

        switch (side) {
            .tail => {
                // Reserve capacity in one call so no append reallocates mid-loop.
                try list_ptr.ensureUnusedCapacity(self.gpa, values.len);
                for (values) |v| {
                    list_ptr.appendAssumeCapacity(try self.gpa.dupe(u8, v));
                }
            },
            .head => {
                // Redis LPUSH prepends each value one at a time, so input
                // [a, b, c] ends up as [c, b, a] at the head. Insert-at-0
                // per iteration makes that ordering visible. It's O(len)
                // per insert on ArrayList; a doubly-linked backing would be
                // O(1). See the List type-alias note at the top of the file.
                for (values) |v| {
                    const dup = try self.gpa.dupe(u8, v);
                    errdefer self.gpa.free(dup);
                    try list_ptr.insert(self.gpa, 0, dup);
                }
            },
        }
        entry_ptr.version = self.nextVersion();

        // RPUSH/LPUSH report the length after append. BLPOP handoff below is a
        // separate event that doesn't refund the push count.
        const push_length = list_ptr.items.len;

        // Hand off elements to BLPOP waiters, FIFO.
        if (self.waiters.getPtr(key)) |waiters_list| {
            while (waiters_list.first) |first_node| {
                if (list_ptr.items.len == 0) break;
                const waiter: *Waiter = @fieldParentPtr("node", first_node);
                waiters_list.remove(first_node);
                const elem = list_ptr.orderedRemove(0);
                waiter.delivered = elem;
                waiter.condition.signal(io);
            }
            if (waiters_list.first == null) {
                if (self.waiters.fetchRemove(key)) |kv| self.gpa.free(kv.key);
            }
        }

        // Delete-on-empty uses actual final length.
        if (list_ptr.items.len == 0) self.removeKey(key);

        return push_length;
    }

    // Pop up to `count` elements from the head or tail of the list at `key`.
    // Returns null when the key is absent/expired, the caller distinguishes
    // "no key" from "popped zero elements from an existing key" (the latter
    // is an empty slice, not null). WrongType when the key holds a non-list.
    // Popped elements are arena-owned; the store's copies are freed. When
    // the pop empties the list, the key is deleted (Redis's "no empty
    // collections" invariant).
    pub fn listPop(self: *Store, out_arena: std.mem.Allocator, key: []const u8, count: usize, side: Side, now_ms: i64) GetError!?[]const []const u8 {
        const entry = self.getLiveEntry(key, now_ms) orelse return null;
        switch (entry.value) {
            .list => |*list| {
                const available = list.items.len;
                const n = @min(count, available);
                const out = try out_arena.alloc([]const u8, n);

                // Phase 1: dupe all n elements into the arena. This is the only
                // fallible work; if it errors partway, the store hasn't been
                // touched yet, so the state stays consistent (partial arena
                // allocs die on the next command's reset).
                switch (side) {
                    .head => {
                        for (0..n) |i| out[i] = try out_arena.dupe(u8, list.items[i]);
                    },
                    .tail => {
                        // Pop order is last-first: RPOP list 2 returns [last, second-last].
                        for (0..n) |i| out[i] = try out_arena.dupe(u8, list.items[available - 1 - i]);
                    },
                }

                // Phase 2: mutate the store. Infallible — free gpa copies, then
                // truncate. Only when Phase 1 fully succeeded do we get here.
                switch (side) {
                    .head => {
                        for (list.items[0..n]) |elem| self.gpa.free(elem);
                        // Shift remaining elements left by n. Forward iteration is
                        // correct because destination [0..) is entirely before source
                        // [n..) — every read happens before its slot is overwritten.
                        for (0..available - n) |i| list.items[i] = list.items[i + n];
                        list.items.len -= n;
                    },
                    .tail => {
                        for (list.items[available - n ..]) |elem| self.gpa.free(elem);
                        list.items.len -= n;
                    },
                }

                // Delete-on-empty. `list` dangles after removeKey, so nothing
                // below may touch it.
                if (list.items.len == 0) {
                    self.removeKey(key);
                } else if (n > 0) {
                    entry.version = self.nextVersion();
                }

                return out;
            },
            else => return error.WrongType,
        }
    }

    pub fn listRange(self: *Store, out_arena: std.mem.Allocator, key: []const u8, start: i64, stop: i64, now_ms: i64) GetError![]const []const u8 {
        const entry = self.getLiveEntry(key, now_ms) orelse return &.{};
        const list = switch (entry.value) {
            .list => |l| l,
            else => return error.WrongType,
        };

        // Translate negative indices ("−k" = "len − k"). Redis clamps a start
        // that's still below zero after translation to 0, and leaves a still-
        // negative stop alone — the `s > e` guard below turns that into an
        // empty reply.
        const len_i: i64 = @intCast(list.items.len);
        const s: i64 = if (start < 0) @max(start + len_i, 0) else start;
        const e: i64 = if (stop < 0) stop + len_i else stop;

        if (s >= len_i or s > e) return &.{};
        const end_incl = @min(e, len_i - 1);

        const begin: usize = @intCast(s);
        const end: usize = @intCast(end_incl + 1);
        const slice = list.items[begin..end];

        const out = try out_arena.alloc([]const u8, slice.len);
        for (slice, out) |elem, *slot| {
            slot.* = try out_arena.dupe(u8, elem);
        }

        return out;
    }

    pub fn listLength(self: *Store, key: []const u8, now_ms: i64) GetError!usize {
        const entry = self.getLiveEntry(key, now_ms) orelse return 0;
        const list = switch (entry.value) {
            .list => |l| l,
            else => return error.WrongType,
        };

        return list.items.len;
    }

    fn enqueueWaiter(self: *Store, key: []const u8, waiter: *Waiter) !void {
        const key_copy = try self.gpa.dupe(u8, key);
        errdefer self.gpa.free(key_copy);

        const gop = try self.waiters.getOrPut(self.gpa, key_copy);
        if (gop.found_existing) {
            self.gpa.free(key_copy);
        } else {
            gop.value_ptr.* = .{};
        }

        gop.value_ptr.append(&waiter.node);
    }

    // Only for a waiter that is still queued. A delivered waiter was already
    // unlinked by listPush, and removing it again would corrupt the list.
    fn dequeueWaiter(self: *Store, key: []const u8, waiter: *Waiter) void {
        const list_ptr = self.waiters.getPtr(key) orelse return;
        list_ptr.remove(&waiter.node);
        if (list_ptr.first == null) {
            if (self.waiters.fetchRemove(key)) |kv| self.gpa.free(kv.key);
        }
    }

    pub const BlockedPop = struct {
        key: []const u8,
        value: []const u8,
    };

    // Pop the head of `key`'s list. If the list is non-empty, act like a
    // non-blocking single-element listPop. If empty or absent, park as a
    // waiter and wait for either a push or the timeout. `timeout_ms == null`
    // means block indefinitely; `0` means don't block at all (BLPOP inside
    // MULTI). Returns null on timeout.
    pub fn listPopBlocking(self: *Store, io: Io, out_arena: std.mem.Allocator, key: []const u8, timeout_ms: ?u64, now_ms: i64) (GetError || Io.Cancelable)!?BlockedPop {
        // Fast path, element is already available. A live list is never
        // empty, so a non-null result always holds exactly one element.
        if (try self.listPop(out_arena, key, 1, .head, now_ms)) |items| {
            return .{ .key = try out_arena.dupe(u8, key), .value = items[0] };
        }
        if (timeout_ms == 0) return null;

        // Slow path, park and wait. The defer runs on every exit, including
        // cancellation: free a delivered element, or unlink if never delivered.
        var waiter: Waiter = .{};
        try self.enqueueWaiter(key, &waiter);
        defer if (waiter.delivered) |d| self.gpa.free(d) else self.dequeueWaiter(key, &waiter);

        // Infinite → condvar (wait releases the mutex while parked). Bounded →
        // poll, since Io.Condition has no timed wait.
        if (timeout_ms) |ms| {
            const deadline_ms = Io.Clock.awake.now(io).toMilliseconds() + @as(i64, @intCast(ms));
            while (waiter.delivered == null) {
                const cur = Io.Clock.awake.now(io).toMilliseconds();
                if (cur >= deadline_ms) break;
                try self.sleepUnlocked(io, @min(50, @as(u64, @intCast(deadline_ms - cur))));
            }
        } else {
            while (waiter.delivered == null) try waiter.condition.wait(io, &self.mutex);
        }

        const delivered = waiter.delivered orelse return null; // timed out
        return .{ .key = try out_arena.dupe(u8, key), .value = try out_arena.dupe(u8, delivered) };
    }

    pub fn streamAdd(self: *Store, key: []const u8, id_spec: StreamIdSpec, fields: []const []const u8, now_ms: i64) StreamAddError!StreamEntryId {
        const fields_copy = try self.gpa.alloc([]const u8, fields.len);
        errdefer self.gpa.free(fields_copy);

        // `duped` is re-read when the errdefer fires, so it always reflects the
        // real count of live copies at unwind time.
        var duped: usize = 0;
        errdefer for (fields_copy[0..duped]) |f| self.gpa.free(f);
        while (duped < fields.len) : (duped += 1) {
            fields_copy[duped] = try self.gpa.dupe(u8, fields[duped]);
        }

        var entry_ptr: *Entry = undefined;
        if (self.getLiveEntry(key, now_ms)) |ep| {
            switch (ep.value) {
                .stream => entry_ptr = ep,
                else => return error.WrongType,
            }
        } else {
            const key_copy = try self.gpa.dupe(u8, key);
            errdefer self.gpa.free(key_copy);

            const gop = try self.map.getOrPut(self.gpa, key_copy);
            gop.value_ptr.* = .{ .value = .{ .stream = .empty }, .expires_at_ms = null, .version = 0 };
            entry_ptr = gop.value_ptr;
        }
        const stream_ptr = &entry_ptr.value.stream;

        // Resolve the id spec against the last entry (if any) and validate.
        // Errors here return without touching the stream, the errdefers up top
        // free the field copies. The empty stream we may have just created stays
        // in the map; matches the same wart listPush has.
        const last: ?StreamEntryId = if (stream_ptr.items.len == 0)
            null
        else
            stream_ptr.items[stream_ptr.items.len - 1].id;
        const id = try resolveStreamId(id_spec, last, now_ms);

        try stream_ptr.append(self.gpa, .{ .id = id, .fields = fields_copy });
        entry_ptr.version = self.nextVersion();
        return id;
    }

    fn resolveStreamId(spec: StreamIdSpec, last: ?StreamEntryId, now_ms: i64) StreamAddError!StreamEntryId {
        const id: StreamEntryId = switch (spec) {
            .explicit => |e| e,
            .ms_auto_seq => |ms| .{ .ms = ms, .seq = nextSeqFor(ms, last) },
            .fully_auto => blk: {
                const now: u64 = if (now_ms < 0) 0 else @intCast(now_ms);
                // If the wall clock went backwards vs. the last entry, don't
                // regress, clamp ms up so we stay strictly monotonic.
                const ms = if (last) |l| @max(l.ms, now) else now;
                break :blk .{ .ms = ms, .seq = nextSeqFor(ms, last) };
            },
        };

        if (id.ms == 0 and id.seq == 0) return error.IdZero;
        if (last) |l| switch (id.order(l)) {
            .gt => {},
            else => return error.IdEqualOrSmaller,
        };

        return id;
    }

    // Next sequence number for a given ms bucket. On an empty bucket the seq
    // starts at 0, except for ms==0 where the 0-0 taboo forces it to 1.
    fn nextSeqFor(ms: u64, last: ?StreamEntryId) u64 {
        if (last) |l| if (l.ms == ms) return l.seq + 1;
        return if (ms == 0) 1 else 0;
    }

    // Inclusive [start, end] scan. Copies matched entries' fields into out_arena
    // so the caller can use them after the store changes.
    pub fn streamRange(self: *Store, out_arena: std.mem.Allocator, key: []const u8, start: StreamEntryId, end: StreamEntryId, now_ms: i64) GetError![]const StreamRangeEntry {
        const entry_ptr = self.getLiveEntry(key, now_ms) orelse return &.{};
        const stream = switch (entry_ptr.value) {
            .stream => |s| s,
            else => return error.WrongType,
        };

        // Two-pass: count matches first so we can alloc the outer slice exactly
        // then dupe. Entries are sorted by construction, so we `break` past `end`
        // instead of scanning the whole stream
        var count: usize = 0;
        for (stream.items) |e| {
            if (e.id.order(start) == .lt) continue;
            if (e.id.order(end) == .gt) break;
            count += 1;
        }

        const out = try out_arena.alloc(StreamRangeEntry, count);
        var i: usize = 0;
        for (stream.items) |e| {
            if (e.id.order(start) == .lt) continue;
            if (e.id.order(end) == .gt) break;
            const fields = try out_arena.alloc([]const u8, e.fields.len);
            for (e.fields, fields) |src, *slot| slot.* = try out_arena.dupe(u8, src);
            out[i] = .{ .id = e.id, .fields = fields };
            i += 1;
        }

        return out;
    }

    pub fn streamLastId(self: *Store, key: []const u8, now_ms: i64) GetError!?StreamEntryId {
        const entry_ptr = self.getLiveEntry(key, now_ms) orelse return null;
        const stream = switch (entry_ptr.value) {
            .stream => |s| s,
            else => return error.WrongType,
        };
        if (stream.items.len == 0) return null;
        return stream.items[stream.items.len - 1].id;
    }
};
