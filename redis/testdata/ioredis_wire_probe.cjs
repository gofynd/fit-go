"use strict";

// Runtime oracle for the pinned ioredis dependency. This script is not part
// of `go test`; point IOREDIS_MODULE at an installed ioredis 5.11.1 directory.
// Example:
//   IOREDIS_MODULE=/tmp/node_modules/ioredis \
//     node redis/testdata/ioredis_wire_probe.cjs startup

const net = require("net");
const modulePath = process.env.IOREDIS_MODULE || "ioredis";
const Redis = require(modulePath);
const version = require(`${modulePath}/package.json`).version;

if (version !== "5.11.1") {
  throw new Error(`expected ioredis 5.11.1, got ${version}`);
}

function parseOne(buffer) {
  if (buffer[0] !== 42) throw new Error("expected RESP array");
  const headEnd = buffer.indexOf("\r\n");
  if (headEnd < 0) return null;
  const count = Number(buffer.subarray(1, headEnd));
  let offset = headEnd + 2;
  const command = [];
  for (let index = 0; index < count; index += 1) {
    if (buffer[offset] !== 36) throw new Error("expected RESP bulk string");
    const lengthEnd = buffer.indexOf("\r\n", offset);
    if (lengthEnd < 0) return null;
    const length = Number(buffer.subarray(offset + 1, lengthEnd));
    const start = lengthEnd + 2;
    const end = start + length;
    if (buffer.length < end + 2) return null;
    command.push(buffer.subarray(start, end).toString());
    offset = end + 2;
  }
  return { command, rest: buffer.subarray(offset) };
}

function ready(redis) {
  return new Promise((resolve, reject) => {
    redis.once("ready", resolve);
    redis.once("error", reject);
  });
}

async function startupProbe() {
  const commands = [];
  const server = net.createServer((socket) => {
    let pending = Buffer.alloc(0);
    socket.on("data", (chunk) => {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length) {
        const parsed = parseOne(pending);
        if (!parsed) return;
        pending = parsed.rest;
        commands.push(parsed.command);
        const name = parsed.command[0].toUpperCase();
        if (name === "CLIENT") socket.write("-ERR unknown CLIENT subcommand\r\n");
        else if (name === "INFO") socket.write("$11\r\nloading:0\r\n\r\n");
        else if (name === "PING") socket.write("+PONG\r\n");
        else socket.write("+OK\r\n");
      }
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;
  const redis = new Redis(`redis://legacy-user:legacy-pass@127.0.0.1:${port}/4`, {
    connectionName: "uat-cache",
    connectTimeout: 10000,
  });
  redis.on("error", () => {});
  await ready(redis);
  const ping = await redis.ping();
  redis.disconnect();
  await new Promise((resolve) => server.close(resolve));
  return { commands, ping };
}

async function lostReplyProbe() {
  const commands = [];
  let connectionSequence = 0;
  const server = net.createServer((socket) => {
    const connection = ++connectionSequence;
    let pending = Buffer.alloc(0);
    let closeScheduled = false;
    socket.on("data", (chunk) => {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length) {
        const parsed = parseOne(pending);
        if (!parsed) return;
        pending = parsed.rest;
        const command = parsed.command;
        commands.push({ connection, command });
        const name = command[0].toUpperCase();
        if (name === "CLIENT") socket.write("-ERR unknown CLIENT subcommand\r\n");
        else if (name === "INFO") socket.write("$11\r\nloading:0\r\n\r\n");
        else if (connection === 1 && name === "INCR") {
          // Read every command already delivered in this data event, then lose
          // every reply. This deterministically exposes ioredis's complete
          // unfulfilled-command replay set.
          if (!closeScheduled) {
            closeScheduled = true;
            setImmediate(() => socket.destroy());
          }
        } else if (name === "INCR") socket.write(":1\r\n");
        else socket.write("+OK\r\n");
      }
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const redis = new Redis(server.address().port, "127.0.0.1", {
    connectionName: "cache",
    retryStrategy: () => 1,
  });
  redis.on("error", () => {});
  await ready(redis);
  const results = await Promise.all([redis.incr("first"), redis.incr("second")]);
  redis.disconnect();
  await new Promise((resolve) => server.close(resolve));
  return { commands, results };
}

const selected = process.argv[2];
const probe = selected === "startup"
  ? startupProbe
  : selected === "lost-reply"
    ? lostReplyProbe
    : null;

if (!probe) {
  throw new Error("usage: ioredis_wire_probe.cjs <startup|lost-reply>");
}

probe().then((result) => process.stdout.write(JSON.stringify(result)));
