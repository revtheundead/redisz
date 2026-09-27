const std = @import("std");

// Server settings from the command line. Real Redis accepts any redis.conf
// directive as "--name value".
pub const Config = struct {
    port: u16 = 6379,

    pub fn parse(args: []const [:0]const u8) !Config {
        var config: Config = .{};
        var i: usize = 1; // args[0] is the program path
        while (i < args.len) : (i += 1) {
            const flag = args[i];
            if (std.mem.eql(u8, flag, "--port") or std.mem.eql(u8, flag, "-p")) {
                const value = try nextValue(args, &i);
                config.port = std.fmt.parseInt(u16, value, 10) catch {
                    std.log.err("invalid port '{s}'", .{value});
                    return error.InvalidArgs;
                };
            } else {
                std.log.err("unknown option '{s}'", .{flag});
                return error.InvalidArgs;
            }
        }
        return config;
    }

    fn nextValue(args: []const [:0]const u8, i: *usize) ![]const u8 {
        i.* += 1;
        if (i.* >= args.len) {
            std.log.err("option '{s}' needs a value", .{args[i.* - 1]});
            return error.InvalidArgs;
        }
        return args[i.*];
    }
};
