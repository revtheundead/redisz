const std = @import("std");
const resp = @import("resp.zig");
const Store = @import("store.zig").Store;
const Io = std.Io;

fn nowMs(io: Io) i64 {
    return Io.Clock.awake.now(io).toMilliseconds();
}

// Top-level command router. Reads args[0] and dispatches to a handler.
// An if/else chain is fine up to ~20 commands; when we outgrow it we'll swap
// in a comptime StaticStringMap.
pub fn dispatch(io: Io, arena: std.mem.Allocator, store: *Store, w: *Io.Writer, value: resp.Value) !void {
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
    } else if (std.ascii.eqlIgnoreCase(cmd, "LRANGE")) {
        try handleLrange(io, arena, w, store, args);
    } else if (std.ascii.eqlIgnoreCase(cmd, "LLEN")) {
        try handleLlen(io, w, store, args);
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
