const std = @import("std");
const resp = @import("resp.zig");
const Store = @import("store.zig").Store;
const Io = std.Io;

fn nowMs(io: Io) i64 {
    return Io.Clock.awake.now(io).toMilliseconds();
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    var store: Store = .init(gpa);
    defer store.deinit();

    // Start server
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 6379);
    var server = try address.listen(io, .{
        .reuse_address = true,
    });
    defer server.deinit(io);

    var clients: Io.Group = .init;
    defer clients.await(io) catch {};

    while (true) {
        const connection = server.accept(io) catch |err| switch (err) {
            error.Canceled => break,
            else => return err,
        };
        try clients.concurrent(io, handleClient, .{ io, connection, &store });
    }
}

fn handleClient(io: Io, stream: Io.net.Stream, store: *Store) Io.Cancelable!void {
    defer stream.close(io);

    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var read_buf: [4096]u8 = undefined;
    var stream_reader = stream.reader(io, &read_buf);
    var stream_writer = stream.writer(io, &.{});

    while (true) {
        _ = arena_state.reset(.retain_capacity);

        const value = resp.parseValue(&stream_reader.interface, arena) catch |err| switch (err) {
            error.ReadFailed => {
                const e = stream_reader.err orelse break;
                if (e == error.Canceled) return error.Canceled;
                break;
            },
            else => break,
        };

        dispatch(io, arena, store, &stream_writer.interface, value) catch break;
    }
}

fn dispatch(io: Io, arena: std.mem.Allocator, store: *Store, w: *Io.Writer, value: resp.Value) !void {
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
        try w.writeAll("+PONG\r\n");
    } else if (std.ascii.eqlIgnoreCase(cmd, "ECHO")) {
        if (args.len < 2) {
            try w.writeAll("-ERR wrong number of arguments for 'echo'\r\n");
            return;
        }
        const arg = switch (args[1]) {
            .bulk_string => |maybe| maybe orelse "",
            else => return,
        };
        try resp.writeBulkString(w, arg);
    } else if (std.ascii.eqlIgnoreCase(cmd, "SET")) {
        if (args.len < 3) return try w.writeAll("-ERR wrong number of arguments for 'set'\r\n");
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
                if (i >= args.len) return try w.writeAll("-ERR syntax error\r\n");
                const ttl_str = switch (args[i]) {
                    .bulk_string => |m| m orelse return,
                    else => return,
                };
                const ttl = std.fmt.parseInt(i64, ttl_str, 10) catch {
                    return try w.writeAll("-ERR value is not an integer or out of range\r\n");
                };
                const ttl_ms = if (std.ascii.eqlIgnoreCase(opt, "PX")) ttl else ttl * 1000;
                expires_at_ms = nowMs(io) + ttl_ms;
            } else {
                return try w.writeAll("-ERR syntax error\r\n");
            }
        }

        try store.set(io, key, val, expires_at_ms);
        try w.writeAll("+OK\r\n");
    } else if (std.ascii.eqlIgnoreCase(cmd, "GET")) {
        if (args.len < 2) return try w.writeAll("-ERR wrong number of arguments for 'get'\r\n");
        const key = switch (args[1]) {
            .bulk_string => |m| m orelse return,
            else => return,
        };
        if (try store.get(io, arena, key, nowMs(io))) |val| {
            try resp.writeBulkString(w, val);
        } else {
            try w.writeAll("$-1\r\n");
        }
    } else {
        try w.print("-ERR unknown command '{s}'\r\n", .{cmd});
    }
}
