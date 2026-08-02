const std = @import("std");
const resp = @import("resp.zig");
const Store = @import("store.zig").Store;
const StreamEntryId = @import("store.zig").StreamEntryId;
const StreamRangeEntry = @import("store.zig").StreamRangeEntry;
const ClientState = @import("main.zig").ClientState;
const Io = std.Io;

fn nowMs(io: Io) i64 {
    return Io.Clock.real.now(io).toMilliseconds();
}

// Top-level command router. Reads args[0] and dispatches to a handler.
// An if/else chain is fine up to ~20 commands; when we outgrow it we'll swap
// in a comptime StaticStringMap.
pub fn dispatch(io: Io, arena: std.mem.Allocator, store: *Store, w: *Io.Writer, value: resp.Value, client: *ClientState) !void {
    const args = switch (value) {
        .array => |maybe| maybe orelse return,
        else => return,
    };
    if (args.len == 0) return;

    const cmd = switch (args[0]) {
        .bulk_string => |maybe| maybe orelse return,
        else => return,
    };

    if (std.ascii.eqlIgnoreCase(cmd, "PING")) {
        try handlePing(w);
    } else if (std.ascii.eqlIgnoreCase(cmd, "ECHO")) {
        try handleEcho(w, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "SET")) {
        try handleSet(io, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "GET")) {
        try handleGet(io, arena, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "RPUSH")) {
        try handleRpush(io, arena, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "LPUSH")) {
        try handleLpush(io, arena, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "LPOP")) {
        try handleLpop(io, arena, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "BLPOP")) {
        try handleBlpop(io, arena, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "LRANGE")) {
        try handleLrange(io, arena, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "LLEN")) {
        try handleLlen(io, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "TYPE")) {
        try handleType(io, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "XADD")) {
        try handleXadd(io, arena, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "XRANGE")) {
        try handleXrange(io, arena, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "XREAD")) {
        try handleXread(io, arena, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "INCR")) {
        try handleIncr(io, arena, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "MULTI")) {
        try handleMulti(w, client, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "EXEC")) {
        try handleExec(w, client, args);
    } else {
        try w.print("-ERR unknown command '{s}'\r\n", .{cmd});
    }
}

fn handlePing(w: *Io.Writer) !void {
    try resp.writeSimpleString(w, "PONG");
}

fn handleEcho(w: *Io.Writer, args: []const resp.Value) !void {
    if (args.len < 2) return try resp.writeError(w, "ERR wrong number of arguments for 'echo'");
    const arg = switch (args[1]) {
        .bulk_string => |m| m orelse "",
        else => return,
    };
    try resp.writeBulkString(w, arg);
}

fn handleSet(io: Io, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    if (args.len < 3) return try resp.writeError(w, "ERR wrong number of arguments for 'set'");
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };
    const val = switch (args[2]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };

    var expires_at_ms: ?i64 = null;
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |m| m orelse return,
            else => return,
        };

        if (std.ascii.eqlIgnoreCase(opt, "PX") or std.ascii.eqlIgnoreCase(opt, "EX")) {
            i += 1;
            if (i >= args.len) return try resp.writeError(w, "ERR syntax error");
            const ttl_str = switch (args[i]) {
                .bulk_string => |m| m orelse return,
                else => return,
            };
            const ttl = std.fmt.parseInt(i64, ttl_str, 10) catch {
                return try resp.writeError(w, "ERR value is not an integer or out of range");
            };
            const ttl_ms = if (std.ascii.eqlIgnoreCase(opt, "PX")) ttl else ttl * 1000;
            expires_at_ms = nowMs(io) + ttl_ms;
        } else {
            return try resp.writeError(w, "ERR syntax error");
        }
    }

    try store.set(io, key, val, expires_at_ms);
    try resp.writeSimpleString(w, "OK");
}

fn handleGet(io: Io, arena: std.mem.Allocator, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    if (args.len < 2) return try resp.writeError(w, "ERR wrong number of arguments for 'get'");
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };
    const maybe_val = store.get(io, arena, key, nowMs(io)) catch |err| switch (err) {
        error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };
    if (maybe_val) |val| {
        try resp.writeBulkString(w, val);
    } else {
        try resp.writeNullBulk(w);
    }
}

fn handleRpush(io: Io, arena: std.mem.Allocator, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    if (args.len < 3) return try resp.writeError(w, "ERR wrong number of arguments for 'rpush'");
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };

    // Unpack the values from args[2..] into a plain slice. Arena-allocated,
    // the store dupes each value with gpa, so nothing here needs to outlive
    // dispatch.
    const values = try arena.alloc([]const u8, args.len - 2);
    for (args[2..], values) |arg, *slot| {
        slot.* = switch (arg) {
            .bulk_string => |m| m orelse return,
            else => return,
        };
    }

    const new_len = store.listPush(io, key, values, .tail, nowMs(io)) catch |err| switch (err) {
        error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };

    try resp.writeInteger(w, @intCast(new_len));
}

fn handleLpush(io: Io, arena: std.mem.Allocator, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    if (args.len < 3) return try resp.writeError(w, "ERR wrong number of arguments for 'lpush'");
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };

    // Unpack the values from args[2..] into a plain slice. Arena-allocated,
    // the store dupes each value with gpa, so nothing here needs to outlive
    // dispatch.
    const values = try arena.alloc([]const u8, args.len - 2);
    for (args[2..], values) |arg, *slot| {
        slot.* = switch (arg) {
            .bulk_string => |m| m orelse return,
            else => return,
        };
    }

    const new_len = store.listPush(io, key, values, .head, nowMs(io)) catch |err| switch (err) {
        error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };

    try resp.writeInteger(w, @intCast(new_len));
}

fn handleLpop(io: Io, arena: std.mem.Allocator, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    if (args.len < 2 or args.len > 3) return try resp.writeError(w, "ERR wrong number of arguments for 'lpop'");
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };

    // Count is optional. Its PRESENCE, not its value, decides the reply
    // shape. LPOP key → bulk-or-null. LPOP key 0 → empty array, not null.
    const count: ?usize = if (args.len == 3) blk: {
        const count_str = switch (args[2]) {
            .bulk_string => |m| m orelse return,
            else => return,
        };
        const parsed = std.fmt.parseInt(i64, count_str, 10) catch {
            return try resp.writeError(w, "ERR value is not an integer or out of range");
        };
        if (parsed < 0) return try resp.writeError(w, "ERR value is out of range, must be positive");
        break :blk @intCast(parsed);
    } else null;

    const maybe_items = store.listPop(io, arena, key, count orelse 1, .head, nowMs(io)) catch |err| switch (err) {
        error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };

    if (count == null) {
        // Single-element mode: bulk-or-null.
        const items = maybe_items orelse return try resp.writeNullBulk(w);
        if (items.len == 0) return try resp.writeNullBulk(w);
        try resp.writeBulkString(w, items[0]);
    } else {
        // Count mode: null-array on absent key, else an array of items.
        const items = maybe_items orelse return try resp.writeNullArray(w);
        try resp.writeArrayHeader(w, items.len);
        for (items) |item| try resp.writeBulkString(w, item);
    }
}

fn handleBlpop(io: Io, arena: std.mem.Allocator, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    // Real BLPOP accepts multiple keys before the timeout: BLPOP k1 k2 ... timeout.
    // Single-key only for now, implement multi-key later.
    if (args.len != 3) return try resp.writeError(w, "ERR wrong number of arguments for 'blpop'");
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };
    const timeout_str = switch (args[2]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };

    const timeout_secs = std.fmt.parseFloat(f64, timeout_str) catch {
        return try resp.writeError(w, "ERR timeout is not a valid float");
    };
    if (!std.math.isFinite(timeout_secs) or timeout_secs < 0) {
        return try resp.writeError(w, "ERR timeout is negative");
    }
    const timeout_ms: ?u64 = if (timeout_secs == 0.0)
        null
    else
        @intFromFloat(timeout_secs * 1000.0);

    const maybe_result = store.listPopBlocking(io, arena, key, timeout_ms, nowMs(io)) catch |err| switch (err) {
        error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };

    if (maybe_result) |result| {
        try resp.writeArrayHeader(w, 2);
        try resp.writeBulkString(w, result.key);
        try resp.writeBulkString(w, result.value);
    } else {
        try resp.writeNullArray(w);
    }
}

fn handleLrange(io: Io, arena: std.mem.Allocator, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    if (args.len < 4) return try resp.writeError(w, "ERR wrong number of arguments for 'lrange'");
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };
    const start_str = switch (args[2]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };
    const stop_str = switch (args[3]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };

    const start = std.fmt.parseInt(i64, start_str, 10) catch {
        return try resp.writeError(w, "ERR value is not an integer or out of range");
    };
    const stop = std.fmt.parseInt(i64, stop_str, 10) catch {
        return try resp.writeError(w, "ERR value is not an integer or out of range");
    };

    const items = store.listRange(io, arena, key, start, stop, nowMs(io)) catch |err| switch (err) {
        error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };

    try resp.writeArrayHeader(w, items.len);
    for (items) |item| try resp.writeBulkString(w, item);
}

fn handleLlen(io: Io, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    if (args.len < 2) return try resp.writeError(w, "ERR wrong number of arguments for 'llen'");
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };

    const len = store.listLength(io, key, nowMs(io)) catch |err| switch (err) {
        error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };

    try resp.writeInteger(w, @intCast(len));
}

fn handleType(io: Io, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    if (args.len != 2) return try resp.writeError(w, "ERR wrong number of arguments for 'type'");
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };

    const maybe_tag = try store.getType(io, key, nowMs(io));
    const name = maybe_tag orelse "none";
    try resp.writeSimpleString(w, name);
}

fn handleXadd(io: Io, arena: std.mem.Allocator, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    // XADD key id field1 value1 [field2 value2 ...]
    // Need key + id + at least one field/value pair, and pairs must be even.
    if (args.len < 5 or (args.len - 3) % 2 != 0) {
        return try resp.writeError(w, "ERR wrong number of arguments for 'xadd'");
    }
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };
    const id_str = switch (args[2]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };

    const id_spec = parseStreamIdSpec(id_str) catch {
        return try resp.writeError(w, "ERR Invalid stream ID specified as stream command argument");
    };

    const fields = try arena.alloc([]const u8, args.len - 3);
    for (args[3..], fields) |arg, *slot| {
        slot.* = switch (arg) {
            .bulk_string => |m| m orelse return,
            else => return,
        };
    }

    const assigned = store.streamAdd(io, key, id_spec, fields, nowMs(io)) catch |err| switch (err) {
        error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.IdZero => return try resp.writeError(w, "ERR The ID specified in XADD must be greater than 0-0"),
        error.IdEqualOrSmaller => return try resp.writeError(w, "ERR The ID specified in XADD is equal or smaller than the target stream top item"),
        else => |e| return e,
    };

    // u64-u64: max 20 + 1 + 20 = 41 bytes
    var buf: [48]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{d}-{d}", .{ assigned.ms, assigned.seq }) catch unreachable;
    try resp.writeBulkString(w, s);
}

fn parseStreamIdSpec(s: []const u8) !@import("store.zig").StreamIdSpec {
    if (std.mem.eql(u8, s, "*")) return .fully_auto;
    const dash = std.mem.indexOfScalar(u8, s, '-') orelse return error.InvalidId;
    const ms = std.fmt.parseInt(u64, s[0..dash], 10) catch return error.InvalidId;
    const rest = s[dash + 1 ..];
    if (std.mem.eql(u8, rest, "*")) return .{ .ms_auto_seq = ms };
    const seq = std.fmt.parseInt(u64, rest, 10) catch return error.InvalidId;
    return .{ .explicit = .{ .ms = ms, .seq = seq } };
}

fn handleXrange(io: Io, arena: std.mem.Allocator, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    if (args.len != 4) return try resp.writeError(w, "ERR wrong number of arguments for 'xrange'");
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };
    const start_str = switch (args[2]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };
    const end_str = switch (args[3]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };

    const start = parseStreamRangeBounds(start_str, 0) catch {
        return try resp.writeError(w, "ERR Invalid stream ID specified as stream command argument");
    };
    const end = parseStreamRangeBounds(end_str, std.math.maxInt(u64)) catch {
        return try resp.writeError(w, "ERR Invalid stream ID specified as stream command argument");
    };

    const entries = store.streamRange(io, arena, key, start, end, nowMs(io)) catch |err| switch (err) {
        error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };

    try resp.writeArrayHeader(w, entries.len);
    var buf: [48]u8 = undefined;
    for (entries) |entry| {
        try resp.writeArrayHeader(w, 2);
        const id_str = std.fmt.bufPrint(&buf, "{d}-{d}", .{ entry.id.ms, entry.id.seq }) catch unreachable;
        try resp.writeBulkString(w, id_str);
        try resp.writeArrayHeader(w, entry.fields.len);
        for (entry.fields) |f| try resp.writeBulkString(w, f);
    }
}

fn handleXread(io: Io, arena: std.mem.Allocator, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    // XREAD STREAMS key1 [key2 ...] id1 [id2 ...]
    if (args.len < 4) return try resp.writeError(w, "ERR wrong number of arguments for 'xread'");

    var block_ms: ?u64 = null;
    var i: usize = 1;
    while (i < args.len) {
        const tok = switch (args[i]) {
            .bulk_string => |m| m orelse return,
            else => return,
        };
        if (std.ascii.eqlIgnoreCase(tok, "STREAMS")) break;
        if (std.ascii.eqlIgnoreCase(tok, "BLOCK")) {
            i += 1;
            if (i >= args.len) return try resp.writeError(w, "ERR syntax error");
            const ms_str = switch (args[i]) {
                .bulk_string => |m| m orelse return,
                else => return,
            };
            const parsed = std.fmt.parseInt(i64, ms_str, 10) catch {
                return try resp.writeError(w, "ERR timeout is not an integer or out of range");
            };
            if (parsed < 0) return try resp.writeError(w, "ERR timeout is negative");
            block_ms = @intCast(parsed);
            i += 1;
            continue;
        }
        return try resp.writeError(w, "ERR syntax error");
    }
    if (i >= args.len) return try resp.writeError(w, "ERR syntax error");
    i += 1; // step past STREAMS
    const rest = args[i..];
    if (rest.len == 0 or rest.len % 2 != 0) {
        return try resp.writeError(w, "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.");
    }
    const n = rest.len / 2;

    // Pull the arg strings out once so we don't re-unpack unions later.
    const keys = try arena.alloc([]const u8, n);
    const ids = try arena.alloc([]const u8, n);
    for (rest[0..n], keys) |arg, *slot| {
        slot.* = switch (arg) {
            .bulk_string => |m| m orelse return,
            else => return,
        };
    }
    for (rest[n..], ids) |arg, *slot| {
        slot.* = switch (arg) {
            .bulk_string => |m| m orelse return,
            else => return,
        };
    }

    // Pre-compute successor IDs
    const start_next = try arena.alloc(?StreamEntryId, n);
    const now0 = nowMs(io);
    for (ids, keys, start_next) |id_str, key, *slot| {
        const parsed = parseXreadStart(id_str) catch {
            return try resp.writeError(w, "ERR Invalid stream ID specified as stream command argument");
        };

        const base: StreamEntryId = switch (parsed) {
            .explicit => |id| id,
            .latest => (store.streamLastId(io, key, now0) catch |err| switch (err) {
                error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
                else => |e| return e,
            }) orelse .{ .ms = 0, .seq = 0 },
        };
        slot.* = nextStreamId(base);
    }

    // Query all streams up front. Empty results are kept in place so indices
    // line up with `keys`; we skip them during the write pass.
    const max_id: StreamEntryId = .{ .ms = std.math.maxInt(u64), .seq = std.math.maxInt(u64) };
    const per_stream = try arena.alloc([]const StreamRangeEntry, n);

    const deadline_awake_ms: ?i64 = if (block_ms) |ms|
        (if (ms == 0) null else Io.Clock.awake.now(io).toMilliseconds() + @as(i64, @intCast(ms)))
    else
        null;

    var non_empty: usize = 0;
    while (true) {
        non_empty = 0;
        const now = nowMs(io);
        for (0..n) |k| {
            const sn = start_next[k] orelse {
                per_stream[k] = &.{};
                continue;
            };
            per_stream[k] = store.streamRange(io, arena, keys[k], sn, max_id, now) catch |err| switch (err) {
                error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
                else => |e| return e,
            };
            if (per_stream[k].len > 0) non_empty += 1;
        }
        if (non_empty > 0) break;
        if (block_ms == null) break; // one-shot

        const cur = Io.Clock.awake.now(io).toMilliseconds();
        if (deadline_awake_ms) |dl| {
            if (cur >= dl) break;
            const remaining: u64 = @intCast(dl - cur);
            try io.sleep(.fromMilliseconds(@min(50, remaining)), .awake);
        } else {
            try io.sleep(.fromMilliseconds(50), .awake);
        }
    }

    // Real Redis omits empty streams from the reply and returns a null array
    // when nothing has entries. Matching that keeps the wire format right for
    // the blocking-XREAD stage later, where "nothing yet" needs to be
    // distinguishable from "here's the data".
    if (non_empty == 0) return try resp.writeNullArray(w);

    try resp.writeArrayHeader(w, non_empty);
    var buf: [48]u8 = undefined;
    for (0..n) |j| {
        if (per_stream[j].len == 0) continue;
        try resp.writeArrayHeader(w, 2);
        try resp.writeBulkString(w, keys[j]);
        try resp.writeArrayHeader(w, per_stream[j].len);
        for (per_stream[j]) |entry| {
            try resp.writeArrayHeader(w, 2);
            const id_str = std.fmt.bufPrint(&buf, "{d}-{d}", .{ entry.id.ms, entry.id.seq }) catch unreachable;
            try resp.writeBulkString(w, id_str);
            try resp.writeArrayHeader(w, entry.fields.len);
            for (entry.fields) |f| try resp.writeBulkString(w, f);
        }
    }
}

fn parseStreamRangeBounds(s: []const u8, default_seq: u64) !StreamEntryId {
    if (default_seq == 0 and std.mem.eql(u8, s, "-")) {
        return .{ .ms = 0, .seq = 0 };
    }
    if (default_seq != 0 and std.mem.eql(u8, s, "+")) {
        return .{ .ms = std.math.maxInt(u64), .seq = std.math.maxInt(u64) };
    }
    if (std.mem.indexOfScalar(u8, s, '-')) |dash| {
        const ms = try std.fmt.parseInt(u64, s[0..dash], 10);
        const seq = try std.fmt.parseInt(u64, s[dash + 1 ..], 10);
        return .{ .ms = ms, .seq = seq };
    }
    const ms = try std.fmt.parseInt(u64, s, 10);
    return .{ .ms = ms, .seq = default_seq };
}

fn nextStreamId(id: StreamEntryId) ?StreamEntryId {
    if (id.seq == std.math.maxInt(u64)) {
        if (id.ms == std.math.maxInt(u64)) return null; // saturated
        return .{ .ms = id.ms + 1, .seq = 0 };
    }
    return .{ .ms = id.ms, .seq = id.seq + 1 };
}

const XreadStart = union(enum) {
    explicit: StreamEntryId,
    latest,
};

fn parseXreadStart(s: []const u8) !XreadStart {
    if (std.mem.eql(u8, s, "$")) return .latest;
    if (std.mem.indexOfScalar(u8, s, '-')) |dash| {
        const ms = try std.fmt.parseInt(u64, s[0..dash], 10);
        const seq = try std.fmt.parseInt(u64, s[dash + 1 ..], 10);
        return .{ .explicit = .{ .ms = ms, .seq = seq } };
    }
    // Redis accepts bare "ms" and treats seq as 0.
    const ms = try std.fmt.parseInt(u64, s, 10);
    return .{ .explicit = .{ .ms = ms, .seq = 0 } };
}

fn handleIncr(io: Io, arena: std.mem.Allocator, w: *Io.Writer, store: *Store, args: []const resp.Value) !void {
    if (args.len != 2) return try resp.writeError(w, "ERR wrong number of arguments for 'incr'");
    const key = switch (args[1]) {
        .bulk_string => |m| m orelse return,
        else => return,
    };

    const maybe_val = store.get(io, arena, key, nowMs(io)) catch |err| switch (err) {
        error.WrongType => return try resp.writeError(w, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };

    const cur: i64 = if (maybe_val) |s|
        (std.fmt.parseInt(i64, s, 10) catch {
            return try resp.writeError(w, "ERR value is not an integer or out of range");
        })
    else
        0;

    const res = std.math.add(i64, cur, 1) catch {
        return try resp.writeError(w, "ERR increment or decrement would overflow");
    };

    // max i64 is 19 digits + sign = 20 digits
    var buf: [20]u8 = undefined;
    const res_str = std.fmt.bufPrint(&buf, "{d}", .{res}) catch unreachable;
    try store.set(io, key, res_str, null);
    try resp.writeInteger(w, res);
}

fn handleMulti(w: *Io.Writer, client: *ClientState, args: []const resp.Value) !void {
    if (args.len != 1) return try resp.writeError(w, "ERR wrong number of arguments for 'multi'");
    if (client.in_multi) return try resp.writeError(w, "ERR MULTI calls can not be nested");
    client.in_multi = true;
    try resp.writeSimpleString(w, "OK");
}

fn handleExec(w: *Io.Writer, client: *ClientState, args: []const resp.Value) !void {
    if (args.len != 1) return try resp.writeError(w, "ERR wrong number of arguments for 'exec'");
    if (!client.in_multi) return try resp.writeError(w, "ERR EXEC without MULTI");
    client.in_multi = false;
    try resp.writeArrayHeader(w, 0);
}
