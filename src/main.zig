const std = @import("std");
const resp = @import("resp.zig");
const Io = std.Io;

pub fn main(init: std.process.Init) !void {
    const io = init.io;

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
        try clients.concurrent(io, handleClient, .{ io, connection });
    }
}

fn handleClient(io: Io, stream: Io.net.Stream) Io.Cancelable!void {
    defer stream.close(io);

    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var read_buf: [4096]u8 = undefined;
    var stream_reader = stream.reader(io, &read_buf);
    var stream_writer = stream.writer(io, &.{});

    while (true) {
        _ = arena_state.reset(.retain_capacity);

        const value = resp.parseValue(&stream_reader.interface, arena) catch break;

        dispatch(&stream_writer.interface, value) catch break;
    }
}

fn dispatch(w: *Io.Writer, value: resp.Value) !void {
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
    } else {
        try w.print("-ERR unknown command '{s}'\r\n", .{cmd});
    }
}
