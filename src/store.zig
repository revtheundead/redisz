const std = @import("std");
const Io = std.Io;

pub const Store = struct {
    gpa: std.mem.Allocator,
    map: std.StringArrayHashMapUnmanaged(Entry),
    mutex: std.Io.Mutex,

    const Entry = struct {
        value: []const u8,
        expires_at_ms: ?i64,
    };

    pub fn init(gpa: std.mem.Allocator) Store {
        return .{
            .gpa = gpa,
            .map = .empty,
            .mutex = .init, // '.init' is a *const*, not a call
        };
    }

    pub fn deinit(self: *Store) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.gpa.free(entry.key_ptr.*);
            self.gpa.free(entry.value_ptr.value);
        }
        self.map.deinit(self.gpa);
    }

    pub fn set(self: *Store, io: Io, key: []const u8, value: []const u8, expires_at_ms: ?i64) !void {
        const key_copy = try self.gpa.dupe(u8, key);
        errdefer self.gpa.free(key_copy);
        const value_copy = try self.gpa.dupe(u8, value);
        errdefer self.gpa.free(value_copy);

        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        const gop = try self.map.getOrPut(self.gpa, key_copy);
        if (gop.found_existing) {
            self.gpa.free(key_copy); // duplicate key; existing one stays
            self.gpa.free(gop.value_ptr.value); // old value drops out
        }
        gop.value_ptr.* = .{ .value = value_copy, .expires_at_ms = expires_at_ms };
    }

    // Returns null if the key is absent. On hit, returns an arena-owned copy. Callers
    // can use it freely after the store's mutex has been released
    pub fn get(self: *Store, io: Io, out_arena: std.mem.Allocator, key: []const u8, now_ms: i64) !?[]const u8 {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        const entry_ptr = self.map.getPtr(key) orelse return null;

        if (entry_ptr.expires_at_ms) |deadline| {
            if (now_ms >= deadline) {
                const kv = self.map.fetchSwapRemove(key).?;
                self.gpa.free(kv.key);
                self.gpa.free(kv.value.value);
                return null;
            }
        }

        return try out_arena.dupe(u8, entry_ptr.value);
    }
};
