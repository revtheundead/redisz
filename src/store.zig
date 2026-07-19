const std = @import("std");
const Io = std.Io;

// Redis list backing. ArrayList gives O(1) tail-push and O(1) LINDEX, at the cost
// of O(n) head-push. Real Redis uses a quicklist (linked list of listpacks);
// swappable behind this alias when it matters.
pub const List = std.ArrayListUnmanaged([]const u8);

// Discriminated payload for a key. String is the only variant today; list, stream,
// hash and zset will land as their Codecrafters sections start.
pub const StoredValue = union(enum) {
    string: []const u8,
    list: List,
};

pub const GetError = error{WrongType} || std.mem.Allocator.Error || Io.Cancelable;

pub const Store = struct {
    gpa: std.mem.Allocator,
    map: std.StringArrayHashMapUnmanaged(Entry),
    mutex: Io.Mutex,
    // Per-key FIFO of blocked clients. Empty entries are removed when the last
    // waiter for a key is dequeued (see dequeueWaiter).
    waiters: std.StringHashMapUnmanaged(std.DoublyLinkedList),

    pub const Side = enum { head, tail };

    const Entry = struct {
        value: StoredValue,
        expires_at_ms: ?i64,
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
        }
    }

    // Caller must hold self.mutex. Runs lazy expiration and returns a pointer
    // to a live entry, or null when the key is absent or just evicted.
    fn getLiveEntry(self: *Store, key: []const u8, now_ms: i64) ?*Entry {
        const entry_ptr = self.map.getPtr(key) orelse return null;
        if (entry_ptr.expires_at_ms) |deadline| {
            if (now_ms >= deadline) {
                const kv = self.map.fetchSwapRemove(key).?;
                self.gpa.free(kv.key);
                freeValue(self.gpa, kv.value.value);
                return null;
            }
        }
        return entry_ptr;
    }

    // SET overwrites any prior value regardless of prior type.
    pub fn set(self: *Store, io: Io, key: []const u8, value: []const u8, expires_at_ms: ?i64) !void {
        const key_copy = try self.gpa.dupe(u8, key);
        errdefer self.gpa.free(key_copy);
        const value_copy = try self.gpa.dupe(u8, value);
        errdefer self.gpa.free(value_copy);

        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        const gop = try self.map.getOrPut(self.gpa, key_copy);
        if (gop.found_existing) {
            self.gpa.free(key_copy);
            freeValue(self.gpa, gop.value_ptr.value);
        }
        gop.value_ptr.* = .{ .value = .{ .string = value_copy }, .expires_at_ms = expires_at_ms };
    }

    // Returns an arena-owned copy of the string value.
    //   null            → key absent or expired
    //   WrongType       → key holds a non-string value (once other variants exist)
    pub fn get(self: *Store, io: Io, out_arena: std.mem.Allocator, key: []const u8, now_ms: i64) GetError!?[]const u8 {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        const entry = self.getLiveEntry(key, now_ms) orelse return null;

        return switch (entry.value) {
            .string => |s| try out_arena.dupe(u8, s),
            else => error.WrongType,
        };
    }

    // Returns the RESP type tag ("string", "list", ...) or null if the key
    // is absent/expired. Callers translate null to "none" for the TYPE command.
    pub fn getType(self: *Store, io: Io, key: []const u8, now_ms: i64) Io.Cancelable!?[]const u8 {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        const entry = self.getLiveEntry(key, now_ms) orelse return null;
        return @tagName(entry.value);
    }

    // Push one or more values to `key` on `side`. Creates an empty list if
    // the key is absent/expired. WrongType if the key holds a non-list value.
    // Returns the new list length.
    pub fn listPush(self: *Store, io: Io, key: []const u8, values: []const []const u8, side: Side, now_ms: i64) GetError!usize {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        var list_ptr: *List = undefined;
        if (self.getLiveEntry(key, now_ms)) |entry_ptr| {
            switch (entry_ptr.value) {
                .list => |*l| list_ptr = l,
                else => return error.WrongType,
            }
        } else {
            const key_copy = try self.gpa.dupe(u8, key);
            errdefer self.gpa.free(key_copy);

            const gop = try self.map.getOrPut(self.gpa, key_copy);
            gop.value_ptr.* = .{ .value = .{ .list = .empty }, .expires_at_ms = null };
            list_ptr = &gop.value_ptr.value.list;
        }

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

        // Hand off elements to BLPOP waiters, FIFO. Each waiter takes one element
        // from the head. Loop stop when we run out of either waiters or elements.
        if (self.waiters.getPtr(key)) |waiters_list| {
            while (waiters_list.first) |first_node| {
                if (list_ptr.items.len == 0) break;
                const waiter: *Waiter = @fieldParentPtr("node", first_node);
                waiters_list.remove(first_node);
                const elem = list_ptr.orderedRemove(0);
                waiter.delivered = elem; // ownership transfers to the waiter
                waiter.condition.signal(io); // wake exactly that one waiter
            }
            if (waiters_list.first == null) {
                if (self.waiters.fetchRemove(key)) |kv| self.gpa.free(kv.key);
            }
        }

        // Delete-on-empty (Redis's "no empty collections" invariant): if
        // waiters drained the whole list, delete the key.
        const final_len = list_ptr.items.len;
        if (final_len == 0) {
            const kv = self.map.fetchSwapRemove(key).?;
            self.gpa.free(kv.key);
            freeValue(self.gpa, kv.value.value);
        }

        return final_len;
    }

    // Pop up to `count` elements from the head or tail of the list at `key`.
    // Returns null when the key is absent/expired, the caller distinguishes
    // "no key" from "popped zero elements from an existing key" (the latter
    // is an empty slice, not null). WrongType when the key holds a non-list.
    // Popped elements are arena-owned; the store's copies are freed. When
    // the pop empties the list, the key is deleted (Redis's "no empty
    // collections" invariant).
    pub fn listPop(self: *Store, io: Io, out_arena: std.mem.Allocator, key: []const u8, count: usize, side: Side, now_ms: i64) GetError!?[]const []const u8 {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

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

                // Delete-on-empty. Same invariant as single-element listPop.
                if (list.items.len == 0) {
                    const kv = self.map.fetchSwapRemove(key).?;
                    self.gpa.free(kv.key);
                    freeValue(self.gpa, kv.value.value);
                }

                return out;
            },
            else => return error.WrongType,
        }
    }

    pub fn listRange(self: *Store, io: Io, out_arena: std.mem.Allocator, key: []const u8, start: i64, stop: i64, now_ms: i64) GetError![]const []const u8 {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

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

    pub fn listLength(self: *Store, io: Io, key: []const u8, now_ms: i64) GetError!usize {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        const entry = self.getLiveEntry(key, now_ms) orelse return 0;
        const list = switch (entry.value) {
            .list => |l| l,
            else => return error.WrongType,
        };

        return list.items.len;
    }

    // Caller must hold self.mutex
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

    // Caller must hold self.mutex. Idempotent: no-op if waiter isn't queued.
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

    fn blpopTimeoutSignaler(io: Io, duration_ms: u64, mutex: *Io.Mutex, cond: *Io.Condition) void {
        // Sleep may return error.Canceled — bail out silently in that case,
        // it means the waiter got its element and cancelled us.
        Io.Clock.awake.sleep(io, .fromMilliseconds(duration_ms)) catch return;
        // Signal under the mutex so the write is synchronized with the waiter's read.
        mutex.lock(io) catch return;
        defer mutex.unlock(io);
        cond.signal(io);
    }

    // Pop the head of `key`'s list. If the list is non-empty, act like a
    // non-blocking single-element listPop. If empty or absent, park as a
    // waiter and wait for either a push or the timeout. `timeout_ms == null`
    // means block indefinitely. Returns null on timeout.
    pub fn listPopBlocking(self: *Store, io: Io, out_arena: std.mem.Allocator, key: []const u8, timeout_ms: ?u64, now_ms: i64) GetError!?BlockedPop {
        try self.mutex.lock(io);
        var mutex_held = true;
        defer if (mutex_held) self.mutex.unlock(io);

        // Fast path, element is already available.
        if (self.getLiveEntry(key, now_ms)) |entry_ptr| {
            switch (entry_ptr.value) {
                .list => |*list| {
                    if (list.items.len > 0) {
                        const out_key = try out_arena.dupe(u8, key);
                        const out_value = try out_arena.dupe(u8, list.items[0]);
                        const removed = list.orderedRemove(0);
                        self.gpa.free(removed);
                        if (list.items.len == 0) {
                            const kv = self.map.fetchSwapRemove(key).?;
                            self.gpa.free(kv.key);
                            freeValue(self.gpa, kv.value.value);
                        }
                        return .{ .key = out_key, .value = out_value };
                    }
                },
                else => return error.WrongType,
            }
        }

        // Slow path, park and wait.
        var waiter: Waiter = .{};
        try self.enqueueWaiter(key, &waiter);

        // Wait for delivery. Infinite → condvar. Bounded → poll: release mutex,
        // sleep briefly, reacquire, check `delivered`. The mutex-held flag guards
        // the outer defer against errors from sleep or lock (which can only occur
        // on task cancellation, at which point the outer teardown is unwinding).
        if (timeout_ms) |ms| {
            const deadline_ms = now_ms + @as(i64, @intCast(ms));
            while (waiter.delivered == null) {
                const cur = Io.Clock.awake.now(io).toMilliseconds();
                if (cur >= deadline_ms) break;

                self.mutex.unlock(io);
                mutex_held = false;
                try io.sleep(.fromMilliseconds(50), .awake);
                try self.mutex.lock(io);
                mutex_held = true;
            }
        } else {
            waiter.condition.wait(io, &self.mutex) catch |err| {
                self.dequeueWaiter(key, &waiter);
                return err;
            };
        }

        // Post-wait: signaler sets delivered before signaling / before we notice
        // on our next poll. If it's set, take the element; else we timed out.
        if (waiter.delivered) |delivered| {
            defer self.gpa.free(delivered);
            const out_key = try out_arena.dupe(u8, key);
            const out_value = try out_arena.dupe(u8, delivered);
            return .{ .key = out_key, .value = out_value };
        }

        self.dequeueWaiter(key, &waiter);
        return null;
    }
};
