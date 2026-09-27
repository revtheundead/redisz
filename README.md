[![progress-banner](https://backend.codecrafters.io/progress/redis/b27a5d4d-c9ed-49c7-8ac3-a356437a21b4)](https://app.codecrafters.io/users/revtheundead?r=2qF)

# redisz

A small Redis server written in Zig 0.16, built for the
["Build Your Own Redis" challenge](https://codecrafters.io/challenges/redis) on CodeCrafters.

Supports strings (with expiry), lists (including `BLPOP`), streams, `INCR`,
and transactions with `MULTI` / `EXEC` / `WATCH`.

## Running

```sh
./your_program.sh
```

The server listens on port 6379.

## Layout

- `src/main.zig`: accepts connections and runs the per-client loop
- `src/resp.zig`: RESP protocol parsing and reply writing
- `src/command.zig`: the command table and handlers
- `src/store.zig`: the in-memory keyspace
