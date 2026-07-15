const std = @import("std");
const Io = std.Io;

// Discriminated payload for a key. String is the only variant today; list, stream,
// hash and zset will land as their Codecrafters sections start.
pub const StoredValue = union(enum) {
    string: []const u8,
};

pub const GetError = error{WrongType} || std.mem.Allocator.Error || Io.Cancelable;

pub const Store = struct {
    gpa: std.mem.Allocator,
    map: std.StringArrayHashMapUnmanaged(Entry),
    mutex: Io.Mutex,

    const Entry = struct {
        value: StoredValue,
        expires_at_ms: ?i64,
    };

    pub fn init(gpa: std.mem.Allocator) Store {
        return .{
            .gpa = gpa,
            .map = .empty,
            .mutex = .init,
        };
    }

    pub fn deinit(self: *Store) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.gpa.free(entry.key_ptr.*);
            freeValue(self.gpa, entry.value_ptr.value);
        }
        self.map.deinit(self.gpa);
    }

    fn freeValue(gpa: std.mem.Allocator, v: StoredValue) void {
        switch (v) {
            .string => |s| gpa.free(s),
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
};
