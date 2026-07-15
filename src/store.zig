const std = @import("std");
const Io = std.Io;

pub const Store = struct {
    gpa: std.mem.Allocator,
    map: std.StringArrayHashMapUnmanaged([]const u8),
    mutex: std.Io.Mutex,

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
            self.gpa.free(entry.value_ptr.*);
        }
        self.map.deinit(self.gpa);
    }

    pub fn set(self: *Store, io: Io, key: []const u8, value: []const u8) !void {
        const key_copy = try self.gpa.dupe(u8, key);
        errdefer self.gpa.free(key_copy);
        const value_copy = try self.gpa.dupe(u8, value);
        errdefer self.gpa.free(value_copy);

        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        const gop = try self.map.getOrPut(self.gpa, key_copy);
        if (gop.found_existing) {
            self.gpa.free(key_copy); // duplicate key; existing one stays
            self.gpa.free(gop.value_ptr.*); // old value drops out
        }
        gop.value_ptr.* = value_copy;
    }

    // Returns null if the key is absent. On hit, returns an arena-owned copy. Callers
    // can use it freely after the store's mutex has been released
    pub fn get(self: *Store, io: Io, out_arena: std.mem.Allocator, key: []const u8) !?[]const u8 {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);

        const value = self.map.get(key) orelse return null;
        return try out_arena.dupe(u8, value);
    }
};
