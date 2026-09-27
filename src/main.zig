const std = @import("std");
const resp = @import("resp.zig");
const Store = @import("store.zig").Store;
const command = @import("command.zig");
const Io = std.Io;

pub const WatchedKey = struct {
    key: []const u8, // gpa-owned dupe; freed by ClientState.clearWatch
    version: u64, // store.watchVersion captured at WATCH time
};

pub const ClientState = struct {
    in_multi: bool = false,
    // Set when a command is rejected while queuing (unknown command, wrong
    // arity). EXEC then discards the whole transaction, like Redis.
    multi_failed: bool = false,
    // Deep gpa copies of each queued command's args. Freed by resetMulti.
    queued: std.ArrayListUnmanaged([]const []const u8) = .empty,
    watched: std.ArrayListUnmanaged(WatchedKey) = .empty,

    // Leaves MULTI mode and drops the queue. Watches are separate, since
    // they're set before MULTI and cleared by EXEC/DISCARD/UNWATCH.
    pub fn resetMulti(self: *ClientState, gpa: std.mem.Allocator) void {
        for (self.queued.items) |args| {
            for (args) |a| gpa.free(a);
            gpa.free(args);
        }
        self.queued.clearRetainingCapacity();
        self.in_multi = false;
        self.multi_failed = false;
    }

    pub fn clearWatch(self: *ClientState, gpa: std.mem.Allocator) void {
        for (self.watched.items) |wk| gpa.free(wk.key);
        self.watched.clearRetainingCapacity();
    }

    fn deinit(self: *ClientState, gpa: std.mem.Allocator) void {
        self.resetMulti(gpa);
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

        // Commands execute one at a time under the store mutex, our stand-in
        // for Redis's single execution thread. The reply is built in memory
        // and sent after unlocking, so a slow client can't stall the rest.
        var reply: Io.Writer.Allocating = .init(arena);
        var ctx: command.Context = .{
            .io = io,
            .arena = arena,
            .store = store,
            .w = &reply.writer,
            .client = &client,
        };

        try store.mutex.lock(io);
        const result = command.dispatch(&ctx, value);
        store.mutex.unlock(io);
        result catch break;

        stream_writer.interface.writeAll(reply.written()) catch break;
    }
}
