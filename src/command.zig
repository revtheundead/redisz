const std = @import("std");
const resp = @import("resp.zig");
const Store = @import("store.zig").Store;
const StreamEntryId = @import("store.zig").StreamEntryId;
const StreamIdSpec = @import("store.zig").StreamIdSpec;
const StreamRangeEntry = @import("store.zig").StreamRangeEntry;
const ClientState = @import("main.zig").ClientState;
const Config = @import("config.zig").Config;
const Io = std.Io;

const not_integer_msg = "ERR value is not an integer or out of range";
const invalid_stream_id_msg = "ERR Invalid stream ID specified as stream command argument";

// Everything a command handler needs. Built per command by the connection
// loop, which holds store.mutex for the whole dispatch.
pub const Context = struct {
    io: Io,
    arena: std.mem.Allocator, // reset after every command
    store: *Store,
    config: *const Config,
    w: *Io.Writer, // collects the reply; sent once the command finishes
    client: *ClientState,
};

const Handler = *const fn (ctx: *Context, args: []const []const u8) anyerror!void;

const Command = struct {
    handler: Handler,
    // Redis convention: arg count including the command name. Positive means
    // exactly that many, negative means at least -arity.
    arity: i32,
    // Runs immediately inside MULTI instead of being queued.
    txn_control: bool = false,
};

const commands = std.StaticStringMapWithEql(Command, std.static_string_map.eqlAsciiIgnoreCase).initComptime(.{
    .{ "ping", Command{ .handler = handlePing, .arity = -1 } },
    .{ "echo", Command{ .handler = handleEcho, .arity = 2 } },
    .{ "info", Command{ .handler = handleInfo, .arity = -1 } },
    .{ "set", Command{ .handler = handleSet, .arity = -3 } },
    .{ "get", Command{ .handler = handleGet, .arity = 2 } },
    .{ "incr", Command{ .handler = handleIncr, .arity = 2 } },
    .{ "type", Command{ .handler = handleType, .arity = 2 } },
    .{ "rpush", Command{ .handler = handleRpush, .arity = -3 } },
    .{ "lpush", Command{ .handler = handleLpush, .arity = -3 } },
    .{ "lpop", Command{ .handler = handleLpop, .arity = -2 } },
    .{ "blpop", Command{ .handler = handleBlpop, .arity = 3 } }, // single key only for now
    .{ "lrange", Command{ .handler = handleLrange, .arity = 4 } },
    .{ "llen", Command{ .handler = handleLlen, .arity = 2 } },
    .{ "xadd", Command{ .handler = handleXadd, .arity = -5 } },
    .{ "xrange", Command{ .handler = handleXrange, .arity = 4 } }, // no COUNT yet
    .{ "xread", Command{ .handler = handleXread, .arity = -4 } },
    .{ "multi", Command{ .handler = handleMulti, .arity = 1, .txn_control = true } },
    .{ "exec", Command{ .handler = handleExec, .arity = 1, .txn_control = true } },
    .{ "discard", Command{ .handler = handleDiscard, .arity = 1, .txn_control = true } },
    .{ "watch", Command{ .handler = handleWatch, .arity = -2, .txn_control = true } },
    .{ "unwatch", Command{ .handler = handleUnwatch, .arity = 1 } },
});

fn nowMs(io: Io) i64 {
    return Io.Clock.real.now(io).toMilliseconds();
}

// Top-level command router. Validates the command against the table, queues
// it when inside MULTI, and otherwise runs its handler.
pub fn dispatch(ctx: *Context, value: resp.Value) !void {
    const args = try toArgs(ctx.arena, value) orelse
        return resp.writeError(ctx.w, "ERR Protocol error: expected an array of bulk strings");
    if (args.len == 0) return;

    const cmd = commands.get(args[0]) orelse
        return rejectCommand(ctx, "ERR unknown command '{s}'", .{args[0]});
    if (!arityOk(cmd.arity, args.len)) {
        const name = try std.ascii.allocLowerString(ctx.arena, args[0]);
        return rejectCommand(ctx, "ERR wrong number of arguments for '{s}' command", .{name});
    }

    if (ctx.client.in_multi and !cmd.txn_control) {
        try enqueueCommand(ctx.store.gpa, ctx.client, args);
        return resp.writeSimpleString(ctx.w, "QUEUED");
    }
    try cmd.handler(ctx, args);
}

// Clients send commands as an array of bulk strings. Returns null for any
// other shape. The slices point into the parser's arena copies.
fn toArgs(arena: std.mem.Allocator, value: resp.Value) !?[]const []const u8 {
    const items = switch (value) {
        .array => |maybe| maybe orelse return null,
        else => return null,
    };
    const args = try arena.alloc([]const u8, items.len);
    for (items, args) |item, *slot| {
        slot.* = switch (item) {
            .bulk_string => |maybe| maybe orelse return null,
            else => return null,
        };
    }
    return args;
}

fn arityOk(arity: i32, argc: usize) bool {
    if (arity >= 0) return argc == @as(usize, @intCast(arity));
    return argc >= @as(usize, @intCast(-arity));
}

// Replies with an error for a command that never ran. Inside MULTI this also
// dooms the transaction: EXEC will refuse to run any of it.
fn rejectCommand(ctx: *Context, comptime fmt: []const u8, fmt_args: anytype) !void {
    if (ctx.client.in_multi) ctx.client.multi_failed = true;
    try resp.writeError(ctx.w, try std.fmt.allocPrint(ctx.arena, fmt, fmt_args));
}

// Replies for the store's user-facing errors. Anything else (OOM,
// cancellation) propagates and closes the connection.
fn writeStoreError(w: *Io.Writer, err: anyerror) anyerror!void {
    const msg = switch (err) {
        error.WrongType => "WRONGTYPE Operation against a key holding the wrong kind of value",
        error.NotInteger => not_integer_msg,
        error.Overflow => "ERR increment or decrement would overflow",
        error.IdZero => "ERR The ID specified in XADD must be greater than 0-0",
        error.IdEqualOrSmaller => "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        else => return err,
    };
    try resp.writeError(w, msg);
}

fn enqueueCommand(gpa: std.mem.Allocator, client: *ClientState, args: []const []const u8) !void {
    // The args live in the per-command arena, so the queue needs its own
    // copies. Two-phase to keep the queue consistent on OOM: dupe everything
    // first, append only when every dupe has succeeded.
    const copy = try gpa.alloc([]const u8, args.len);
    errdefer gpa.free(copy);

    var duped: usize = 0;
    errdefer for (copy[0..duped]) |a| gpa.free(a);
    while (duped < args.len) : (duped += 1) {
        copy[duped] = try gpa.dupe(u8, args[duped]);
    }

    try client.queued.append(gpa, copy);
}

fn handlePing(ctx: *Context, args: []const []const u8) anyerror!void {
    _ = args;
    try resp.writeSimpleString(ctx.w, "PONG");
}

fn handleEcho(ctx: *Context, args: []const []const u8) anyerror!void {
    try resp.writeBulkString(ctx.w, args[1]);
}

// INFO [section ...]. Replies with one bulk string of `key:value` lines,
// grouped under `# Section` headers.
fn handleInfo(ctx: *Context, args: []const []const u8) anyerror!void {
    var out: Io.Writer.Allocating = .init(ctx.arena);
    const info = &out.writer;

    if (wantsSection(args, "replication")) {
        try info.writeAll("# Replication\r\n");
        try info.writeAll("role:master\r\n"); // becomes "slave" with --replicaof
    }

    try resp.writeBulkString(ctx.w, out.written());
}

// Plain INFO (no arguments) shows every section we have
fn wantsSection(args: []const []const u8, name: []const u8) bool {
    if (args.len == 1) return true;
    for (args[1..]) |section| {
        if (std.ascii.eqlIgnoreCase(section, name)) return true;
    }
    return false;
}

fn handleSet(ctx: *Context, args: []const []const u8) anyerror!void {
    var expires_at_ms: ?i64 = null;
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = args[i];
        if (std.ascii.eqlIgnoreCase(opt, "PX") or std.ascii.eqlIgnoreCase(opt, "EX")) {
            i += 1;
            if (i >= args.len) return resp.writeError(ctx.w, "ERR syntax error");
            const ttl = std.fmt.parseInt(i64, args[i], 10) catch return resp.writeError(ctx.w, not_integer_msg);
            const ttl_ms = if (std.ascii.eqlIgnoreCase(opt, "PX")) ttl else ttl * 1000;
            expires_at_ms = nowMs(ctx.io) + ttl_ms;
        } else {
            return resp.writeError(ctx.w, "ERR syntax error");
        }
    }

    try ctx.store.set(args[1], args[2], expires_at_ms);
    try resp.writeSimpleString(ctx.w, "OK");
}

fn handleGet(ctx: *Context, args: []const []const u8) anyerror!void {
    const maybe_val = ctx.store.get(ctx.arena, args[1], nowMs(ctx.io)) catch |err| return writeStoreError(ctx.w, err);
    if (maybe_val) |val| {
        try resp.writeBulkString(ctx.w, val);
    } else {
        try resp.writeNullBulk(ctx.w);
    }
}

fn handleIncr(ctx: *Context, args: []const []const u8) anyerror!void {
    const res = ctx.store.incrBy(args[1], 1, nowMs(ctx.io)) catch |err| return writeStoreError(ctx.w, err);
    try resp.writeInteger(ctx.w, res);
}

fn handleType(ctx: *Context, args: []const []const u8) anyerror!void {
    const name = ctx.store.getType(args[1], nowMs(ctx.io)) orelse "none";
    try resp.writeSimpleString(ctx.w, name);
}

fn handleRpush(ctx: *Context, args: []const []const u8) anyerror!void {
    try push(ctx, args, .tail);
}

fn handleLpush(ctx: *Context, args: []const []const u8) anyerror!void {
    try push(ctx, args, .head);
}

fn push(ctx: *Context, args: []const []const u8, side: Store.Side) !void {
    const new_len = ctx.store.listPush(ctx.io, args[1], args[2..], side, nowMs(ctx.io)) catch |err| return writeStoreError(ctx.w, err);
    try resp.writeInteger(ctx.w, @intCast(new_len));
}

fn handleLpop(ctx: *Context, args: []const []const u8) anyerror!void {
    if (args.len > 3) return resp.writeError(ctx.w, "ERR wrong number of arguments for 'lpop' command");

    // Count is optional. Its PRESENCE, not its value, decides the reply
    // shape. LPOP key → bulk-or-null. LPOP key 0 → empty array, not null.
    const count: ?usize = if (args.len == 3) blk: {
        const parsed = std.fmt.parseInt(i64, args[2], 10) catch return resp.writeError(ctx.w, not_integer_msg);
        if (parsed < 0) return resp.writeError(ctx.w, "ERR value is out of range, must be positive");
        break :blk @intCast(parsed);
    } else null;

    const maybe_items = ctx.store.listPop(ctx.arena, args[1], count orelse 1, .head, nowMs(ctx.io)) catch |err| return writeStoreError(ctx.w, err);

    if (count == null) {
        // Single-element mode: bulk-or-null.
        const items = maybe_items orelse return resp.writeNullBulk(ctx.w);
        if (items.len == 0) return resp.writeNullBulk(ctx.w);
        try resp.writeBulkString(ctx.w, items[0]);
    } else {
        // Count mode: null-array on absent key, else an array of items.
        const items = maybe_items orelse return resp.writeNullArray(ctx.w);
        try resp.writeArrayHeader(ctx.w, items.len);
        for (items) |item| try resp.writeBulkString(ctx.w, item);
    }
}

fn handleBlpop(ctx: *Context, args: []const []const u8) anyerror!void {
    // Real BLPOP accepts multiple keys before the timeout: BLPOP k1 k2 ... timeout.
    // Single-key only for now, implement multi-key later.
    const timeout_secs = std.fmt.parseFloat(f64, args[2]) catch {
        return resp.writeError(ctx.w, "ERR timeout is not a float or out of range");
    };
    if (!std.math.isFinite(timeout_secs) or timeout_secs < 0) {
        return resp.writeError(ctx.w, "ERR timeout is negative");
    }

    // Redis never blocks inside a transaction (it would stall every other
    // client); an empty list gives the timeout reply straight away. A zero
    // timeout from the client means "forever".
    const timeout_ms: ?u64 = if (ctx.client.in_multi)
        0
    else if (timeout_secs == 0.0)
        null
    else
        @intFromFloat(@ceil(timeout_secs * 1000.0));

    const maybe_result = ctx.store.listPopBlocking(ctx.io, ctx.arena, args[1], timeout_ms, nowMs(ctx.io)) catch |err| return writeStoreError(ctx.w, err);

    if (maybe_result) |result| {
        try resp.writeArrayHeader(ctx.w, 2);
        try resp.writeBulkString(ctx.w, result.key);
        try resp.writeBulkString(ctx.w, result.value);
    } else {
        try resp.writeNullArray(ctx.w);
    }
}

fn handleLrange(ctx: *Context, args: []const []const u8) anyerror!void {
    const start = std.fmt.parseInt(i64, args[2], 10) catch return resp.writeError(ctx.w, not_integer_msg);
    const stop = std.fmt.parseInt(i64, args[3], 10) catch return resp.writeError(ctx.w, not_integer_msg);

    const items = ctx.store.listRange(ctx.arena, args[1], start, stop, nowMs(ctx.io)) catch |err| return writeStoreError(ctx.w, err);

    try resp.writeArrayHeader(ctx.w, items.len);
    for (items) |item| try resp.writeBulkString(ctx.w, item);
}

fn handleLlen(ctx: *Context, args: []const []const u8) anyerror!void {
    const len = ctx.store.listLength(args[1], nowMs(ctx.io)) catch |err| return writeStoreError(ctx.w, err);
    try resp.writeInteger(ctx.w, @intCast(len));
}

fn handleXadd(ctx: *Context, args: []const []const u8) anyerror!void {
    // XADD key id field1 value1 [field2 value2 ...]; pairs must be even.
    if ((args.len - 3) % 2 != 0) return resp.writeError(ctx.w, "ERR wrong number of arguments for 'xadd' command");

    const id_spec = parseStreamIdSpec(args[2]) catch return resp.writeError(ctx.w, invalid_stream_id_msg);

    const assigned = ctx.store.streamAdd(args[1], id_spec, args[3..], nowMs(ctx.io)) catch |err| return writeStoreError(ctx.w, err);

    // u64-u64: max 20 + 1 + 20 = 41 bytes
    var buf: [48]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{d}-{d}", .{ assigned.ms, assigned.seq }) catch unreachable;
    try resp.writeBulkString(ctx.w, s);
}

fn parseStreamIdSpec(s: []const u8) !StreamIdSpec {
    if (std.mem.eql(u8, s, "*")) return .fully_auto;
    const dash = std.mem.indexOfScalar(u8, s, '-') orelse return error.InvalidId;
    const ms = std.fmt.parseInt(u64, s[0..dash], 10) catch return error.InvalidId;
    const rest = s[dash + 1 ..];
    if (std.mem.eql(u8, rest, "*")) return .{ .ms_auto_seq = ms };
    const seq = std.fmt.parseInt(u64, rest, 10) catch return error.InvalidId;
    return .{ .explicit = .{ .ms = ms, .seq = seq } };
}

fn handleXrange(ctx: *Context, args: []const []const u8) anyerror!void {
    const start = parseStreamRangeBounds(args[2], 0) catch return resp.writeError(ctx.w, invalid_stream_id_msg);
    const end = parseStreamRangeBounds(args[3], std.math.maxInt(u64)) catch return resp.writeError(ctx.w, invalid_stream_id_msg);

    const entries = ctx.store.streamRange(ctx.arena, args[1], start, end, nowMs(ctx.io)) catch |err| return writeStoreError(ctx.w, err);

    try resp.writeArrayHeader(ctx.w, entries.len);
    for (entries) |entry| try writeStreamEntry(ctx.w, entry);
}

fn writeStreamEntry(w: *Io.Writer, entry: StreamRangeEntry) !void {
    var buf: [48]u8 = undefined;
    try resp.writeArrayHeader(w, 2);
    const id_str = std.fmt.bufPrint(&buf, "{d}-{d}", .{ entry.id.ms, entry.id.seq }) catch unreachable;
    try resp.writeBulkString(w, id_str);
    try resp.writeArrayHeader(w, entry.fields.len);
    for (entry.fields) |f| try resp.writeBulkString(w, f);
}

fn handleXread(ctx: *Context, args: []const []const u8) anyerror!void {
    // XREAD [BLOCK ms] STREAMS key1 [key2 ...] id1 [id2 ...]
    var block_ms: ?u64 = null;
    var i: usize = 1;
    while (i < args.len) {
        const tok = args[i];
        if (std.ascii.eqlIgnoreCase(tok, "STREAMS")) break;
        if (std.ascii.eqlIgnoreCase(tok, "BLOCK")) {
            i += 1;
            if (i >= args.len) return resp.writeError(ctx.w, "ERR syntax error");
            const parsed = std.fmt.parseInt(i64, args[i], 10) catch {
                return resp.writeError(ctx.w, "ERR timeout is not an integer or out of range");
            };
            if (parsed < 0) return resp.writeError(ctx.w, "ERR timeout is negative");
            block_ms = @intCast(parsed);
            i += 1;
            continue;
        }
        return resp.writeError(ctx.w, "ERR syntax error");
    }
    if (i >= args.len) return resp.writeError(ctx.w, "ERR syntax error");
    i += 1; // step past STREAMS
    const rest = args[i..];
    if (rest.len == 0 or rest.len % 2 != 0) {
        return resp.writeError(ctx.w, "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.");
    }
    const n = rest.len / 2;
    const keys = rest[0..n];
    const ids = rest[n..];

    // Same rule as BLPOP: no blocking inside a transaction.
    if (ctx.client.in_multi) block_ms = null;

    // Pre-compute successor IDs
    const start_next = try ctx.arena.alloc(?StreamEntryId, n);
    const now0 = nowMs(ctx.io);
    for (ids, keys, start_next) |id_str, key, *slot| {
        const parsed = parseXreadStart(id_str) catch return resp.writeError(ctx.w, invalid_stream_id_msg);

        const base: StreamEntryId = switch (parsed) {
            .explicit => |id| id,
            .latest => (ctx.store.streamLastId(key, now0) catch |err| return writeStoreError(ctx.w, err)) orelse .{ .ms = 0, .seq = 0 },
        };
        slot.* = nextStreamId(base);
    }

    // Query all streams up front. Empty results are kept in place so indices
    // line up with `keys`; we skip them during the write pass.
    const max_id: StreamEntryId = .{ .ms = std.math.maxInt(u64), .seq = std.math.maxInt(u64) };
    const per_stream = try ctx.arena.alloc([]const StreamRangeEntry, n);

    const deadline_awake_ms: ?i64 = if (block_ms) |ms|
        (if (ms == 0) null else Io.Clock.awake.now(ctx.io).toMilliseconds() + @as(i64, @intCast(ms)))
    else
        null;

    var non_empty: usize = 0;
    while (true) {
        non_empty = 0;
        const now = nowMs(ctx.io);
        for (0..n) |k| {
            const sn = start_next[k] orelse {
                per_stream[k] = &.{};
                continue;
            };
            per_stream[k] = ctx.store.streamRange(ctx.arena, keys[k], sn, max_id, now) catch |err| return writeStoreError(ctx.w, err);
            if (per_stream[k].len > 0) non_empty += 1;
        }
        if (non_empty > 0) break;
        if (block_ms == null) break; // one-shot

        // Poll: release the store so writers can get in, then re-check.
        const cur = Io.Clock.awake.now(ctx.io).toMilliseconds();
        if (deadline_awake_ms) |dl| {
            if (cur >= dl) break;
            const remaining: u64 = @intCast(dl - cur);
            try ctx.store.sleepUnlocked(ctx.io, @min(50, remaining));
        } else {
            try ctx.store.sleepUnlocked(ctx.io, 50);
        }
    }

    // Real Redis omits empty streams from the reply and returns a null array
    // when nothing has entries, so "nothing yet" is distinguishable from
    // "here's the data" for blocking reads.
    if (non_empty == 0) return resp.writeNullArray(ctx.w);

    try resp.writeArrayHeader(ctx.w, non_empty);
    for (0..n) |j| {
        if (per_stream[j].len == 0) continue;
        try resp.writeArrayHeader(ctx.w, 2);
        try resp.writeBulkString(ctx.w, keys[j]);
        try resp.writeArrayHeader(ctx.w, per_stream[j].len);
        for (per_stream[j]) |entry| try writeStreamEntry(ctx.w, entry);
    }
}

fn parseStreamRangeBounds(s: []const u8, default_seq: u64) !StreamEntryId {
    if (default_seq == 0 and std.mem.eql(u8, s, "-")) {
        return .{ .ms = 0, .seq = 0 };
    }
    if (default_seq != 0 and std.mem.eql(u8, s, "+")) {
        return .{ .ms = std.math.maxInt(u64), .seq = std.math.maxInt(u64) };
    }
    if (std.mem.indexOfScalar(u8, s, '-')) |dash| {
        const ms = try std.fmt.parseInt(u64, s[0..dash], 10);
        const seq = try std.fmt.parseInt(u64, s[dash + 1 ..], 10);
        return .{ .ms = ms, .seq = seq };
    }
    const ms = try std.fmt.parseInt(u64, s, 10);
    return .{ .ms = ms, .seq = default_seq };
}

fn nextStreamId(id: StreamEntryId) ?StreamEntryId {
    if (id.seq == std.math.maxInt(u64)) {
        if (id.ms == std.math.maxInt(u64)) return null; // saturated
        return .{ .ms = id.ms + 1, .seq = 0 };
    }
    return .{ .ms = id.ms, .seq = id.seq + 1 };
}

const XreadStart = union(enum) {
    explicit: StreamEntryId,
    latest,
};

fn parseXreadStart(s: []const u8) !XreadStart {
    if (std.mem.eql(u8, s, "$")) return .latest;
    if (std.mem.indexOfScalar(u8, s, '-')) |dash| {
        const ms = try std.fmt.parseInt(u64, s[0..dash], 10);
        const seq = try std.fmt.parseInt(u64, s[dash + 1 ..], 10);
        return .{ .explicit = .{ .ms = ms, .seq = seq } };
    }
    // Redis accepts bare "ms" and treats seq as 0.
    const ms = try std.fmt.parseInt(u64, s, 10);
    return .{ .explicit = .{ .ms = ms, .seq = 0 } };
}

fn handleMulti(ctx: *Context, args: []const []const u8) anyerror!void {
    _ = args;
    if (ctx.client.in_multi) return resp.writeError(ctx.w, "ERR MULTI calls can not be nested");
    ctx.client.in_multi = true;
    try resp.writeSimpleString(ctx.w, "OK");
}

fn handleExec(ctx: *Context, args: []const []const u8) anyerror!void {
    _ = args;
    const client = ctx.client;
    const gpa = ctx.store.gpa;
    if (!client.in_multi) return resp.writeError(ctx.w, "ERR EXEC without MULTI");

    // The queued commands run with in_multi still set, so blocking commands
    // see it and don't block. The defers run even if a command fails midway,
    // leaving the client clean. Watches are always flushed after EXEC.
    defer client.resetMulti(gpa);
    defer client.clearWatch(gpa);

    if (client.multi_failed) {
        return resp.writeError(ctx.w, "EXECABORT Transaction discarded because of previous errors.");
    }

    // Optimistic-lock gate: if any watched key changed since WATCH, the whole
    // transaction is discarded and EXEC replies with a null array. We hold
    // the store mutex from this check through the last queued command, so
    // nothing can slip in between.
    if (watchDirty(ctx)) return resp.writeNullArray(ctx.w);

    try resp.writeArrayHeader(ctx.w, client.queued.items.len);
    for (client.queued.items) |queued_args| {
        const cmd = commands.get(queued_args[0]).?; // validated when queued
        try cmd.handler(ctx, queued_args);
    }
}

// True if any watched key's current version differs from what WATCH recorded.
fn watchDirty(ctx: *Context) bool {
    const now = nowMs(ctx.io);
    for (ctx.client.watched.items) |wk| {
        if (ctx.store.watchVersion(wk.key, now) != wk.version) return true;
    }
    return false;
}

fn handleDiscard(ctx: *Context, args: []const []const u8) anyerror!void {
    _ = args;
    if (!ctx.client.in_multi) return resp.writeError(ctx.w, "ERR DISCARD without MULTI");
    ctx.client.resetMulti(ctx.store.gpa);
    ctx.client.clearWatch(ctx.store.gpa);
    try resp.writeSimpleString(ctx.w, "OK");
}

fn handleWatch(ctx: *Context, args: []const []const u8) anyerror!void {
    if (ctx.client.in_multi) return resp.writeError(ctx.w, "ERR WATCH inside MULTI is not allowed");

    const gpa = ctx.store.gpa;
    const now = nowMs(ctx.io);
    for (args[1..]) |key| {
        const version = ctx.store.watchVersion(key, now);

        // Key must outlive this dispatch (freed at EXEC/DISCARD/UNWATCH)
        const key_copy = try gpa.dupe(u8, key);
        errdefer gpa.free(key_copy);
        try ctx.client.watched.append(gpa, .{ .key = key_copy, .version = version });
    }
    try resp.writeSimpleString(ctx.w, "OK");
}

fn handleUnwatch(ctx: *Context, args: []const []const u8) anyerror!void {
    _ = args;
    ctx.client.clearWatch(ctx.store.gpa);
    try resp.writeSimpleString(ctx.w, "OK");
}
