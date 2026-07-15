const std = @import("std");
const Io = std.Io;

// Parsed RESP2 value. RESP3 additions (map, set, big number, verbatim, push,
// double, boolean, null-type) will extend this union — the shape is additive.
pub const Value = union(enum) {
    simple_string: []const u8,
    err: []const u8,
    integer: i64,
    bulk_string: ?[]const u8, // null = "$-1\r\n" (Null Bulk String)
    array: ?[]const Value, // null = "*-1\r\n" (Null Array)
};

pub const ParseError = error{
    Malformed,
    Overflow,
    InvalidCharacter,
} || Io.Reader.DelimiterError || std.mem.Allocator.Error;

// ---- Parsing --------------------------------------------------------------

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

fn takeCrlfLine(r: *Io.Reader) Io.Reader.DelimiterError![]const u8 {
    const line = try r.takeDelimiterInclusive('\n');
    return std.mem.trimEnd(u8, line, "\r\n");
}

fn takeLineCopy(r: *Io.Reader, arena: std.mem.Allocator) ParseError![]const u8 {
    return arena.dupe(u8, try takeCrlfLine(r));
}

fn parseIntegerLine(r: *Io.Reader) ParseError!i64 {
    return std.fmt.parseInt(i64, try takeCrlfLine(r), 10);
}

fn parseBulkString(r: *Io.Reader, arena: std.mem.Allocator) ParseError!?[]const u8 {
    const len = try parseIntegerLine(r);
    if (len < 0) return null;
    const payload_len: usize = @intCast(len);
    const payload = try r.take(payload_len);
    const owned = try arena.dupe(u8, payload);
    _ = try r.take(2); // consume trailing \r\n
    return owned;
}

fn parseArray(r: *Io.Reader, arena: std.mem.Allocator) ParseError!?[]const Value {
    const len = try parseIntegerLine(r);
    if (len < 0) return null;
    const count: usize = @intCast(len);
    const items = try arena.alloc(Value, count);
    for (items) |*slot| slot.* = try parseValue(r, arena);
    return items;
}

// ---- Reply writers --------------------------------------------------------
// Every reply the server emits should go through one of these so the wire
// format lives in a single file. RESP3 changes null encoding for maps/sets —
// updating that later means editing here, not every command handler.

pub fn writeSimpleString(w: *Io.Writer, s: []const u8) Io.Writer.Error!void {
    try w.writeAll("+");
    try w.writeAll(s);
    try w.writeAll("\r\n");
}

// Caller supplies the full error message including the code prefix,
// e.g. "ERR syntax error" or "WRONGTYPE Operation against a key ...".
pub fn writeError(w: *Io.Writer, msg: []const u8) Io.Writer.Error!void {
    try w.writeAll("-");
    try w.writeAll(msg);
    try w.writeAll("\r\n");
}

pub fn writeInteger(w: *Io.Writer, n: i64) Io.Writer.Error!void {
    try w.print(":{d}\r\n", .{n});
}

pub fn writeBulkString(w: *Io.Writer, s: []const u8) Io.Writer.Error!void {
    try w.print("${d}\r\n", .{s.len});
    try w.writeAll(s);
    try w.writeAll("\r\n");
}

pub fn writeNullBulk(w: *Io.Writer) Io.Writer.Error!void {
    try w.writeAll("$-1\r\n");
}

pub fn writeNullArray(w: *Io.Writer) Io.Writer.Error!void {
    try w.writeAll("*-1\r\n");
}

pub fn writeArrayHeader(w: *Io.Writer, len: usize) Io.Writer.Error!void {
    try w.print("*{d}\r\n", .{len});
}
