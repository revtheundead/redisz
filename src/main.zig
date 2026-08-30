const std = @import("std");
const resp = @import("resp.zig");
const Store = @import("store.zig").Store;
const command = @import("command.zig");
const Io = std.Io;

pub const QueuedCommand = struct {
    // Deep gpa copy of args from the client. Outer slice owned by gpa; each
    // inner slice owned by gpa. Freed by ClientState.clearQueue.
    args: []const []const u8,
};

pub const WatchedKey = struct {
    key: []const u8, // gpa-owned dupe; freed by ClientState.clearWatch
    version: u64, // store.watchVersion captured at WATCH time
};

pub const ClientState = struct {
    in_multi: bool = false,
    queued: std.ArrayListUnmanaged(QueuedCommand) = .empty,
    watched: std.ArrayListUnmanaged(WatchedKey) = .empty,

    pub fn clearQueue(self: *ClientState, gpa: std.mem.Allocator) void {
        for (self.queued.items) |cmd| {
            for (cmd.args) |a| gpa.free(a);
            gpa.free(cmd.args);
        }
        self.queued.clearRetainingCapacity();
    }

    pub fn clearWatch(self: *ClientState, gpa: std.mem.Allocator) void {
        for (self.watched.items) |wk| gpa.free(wk.key);
        self.watched.clearRetainingCapacity();
    }

    fn deinit(self: *ClientState, gpa: std.mem.Allocator) void {
        self.clearQueue(gpa);
        self.queued.deinit(gpa);
        self.clearWatch(gpa);
        self.watched.deinit(gpa);
    }
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = init.gpa;

    var store: Store = .init(gpa);
    defer store.deinit();

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

    var client: ClientState = .{};
    defer client.deinit(store.gpa);

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

        command.dispatch(io, arena, store, &stream_writer.interface, value, &client) catch break;
    }
}
