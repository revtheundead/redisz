const std = @import("std");
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

fn handleClient(io: Io, connection: Io.net.Server.Connection) Io.Cancelable!void {
    defer connection.close(io);

    var connection_writer = connection.writer(io, &.{});
    var buf: [1024]u8 = undefined;
    var data = [_][]u8{&buf};
    while (true) {
        const bytes_read = io.vtable.netRead(io.userdata, connection.socket.handle, &data) catch break;
        if (bytes_read == 0) break;
        connection_writer.interface.writeAll("+PONG\r\n") catch break;
    }
}
