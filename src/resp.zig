const std = @import("std");
const Io = std.Io;

// Value union
pub const Value = union(enum) {
    simple_string: []const u8,
    err: []const u8,
    integer: i64,
    bulk_string: ?[]const u8, // null = "$-1\r\n" (Null Bulk String)
    array: ?[]const Value, // null = "*-1\r\n" (Null Array)
};

// Parse error type
pub const ParseError = error{
    Malformed,
    Overflow,
    InvalidCharacter,
} || Io.Reader.DelimiterError || std.mem.Allocator.Error;

// Main entry to parsing RESP strings
pub fn parseValue(r: *Io.Reader, arena: std.mem.Allocator) ParseError!Value {
    const type_byte = try r.takeByte();
    return switch (type_byte) {
        '+' => .{ .simple_string = try takeLineCopy(r, arena) },
        '-' => .{ .err = try takeLineCopy(r, arena) },
        ':' => .{ .integer = try parseIntegerLine(r) },
        '$' => .{ .bulk_string = try parseBulkString(r, arena) },
        '*' => .{ .array = try parseArray(r, arena) },
        else => error.Malformed,
    };
}

// Takes copies of strings after trimming "\r\n"
fn takeLineCopy(r: *Io.Reader, arena: std.mem.Allocator) ParseError![]const u8 {
    const line = try r.takeDelimiterExclusive('\n');
    const trimmed = std.mem.trimEnd(u8, line, "\r");
    return arena.dupe(u8, trimmed);
}

// Parses an integer line
fn parseIntegerLine(r: *Io.Reader) ParseError!i64 {
    const line = try r.takeDelimiterExclusive('\n');
    const trimmed = std.mem.trimEnd(u8, line, "\r");
    return std.fmt.parseInt(i64, trimmed, 10);
}

// Parses bulk strings
fn parseBulkString(r: *Io.Reader, arena: std.mem.Allocator) ParseError!?[]const u8 {
    const len = try parseIntegerLine(r);
    if (len < 0) return null;
    const payload_len: usize = @intCast(len);
    const payload = try r.take(payload_len);
    const owned = try arena.dupe(u8, payload);
    _ = try r.take(2); // consume trailing \r\n
    return owned;
}

// Parses arrays
fn parseArray(r: *Io.Reader, arena: std.mem.Allocator) ParseError!?[]const Value {
    const len = try parseIntegerLine(r);
    if (len < 0) return null;
    const count: usize = @intCast(len);
    const items = try arena.alloc(Value, count);
    for (items) |*slot| slot.* = try parseValue(r, arena);
    return items;
}

// Allows other modules to write bulk strings
pub fn writeBulkString(w: *Io.Writer, s: []const u8) Io.Writer.Error!void {
    try w.print("${d}\r\n", .{s.len});
    try w.writeAll(s);
    try w.writeAll("\r\n");
}
