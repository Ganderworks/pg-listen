<h1 align="center">pg-listen</h1>
<h3 align="center">Postgres LISTEN & NOTIFY that works</h3>

<p align="center">
  <a href="https://travis-ci.org/andywer/pg-listen">
    <img alt="Build Status" src="https://travis-ci.org/andywer/pg-listen.svg?branch=master" />
  </a>
  <a href="https://www.npmjs.com/package/pg-listen">
    <img alt="NPM Version" src="https://img.shields.io/npm/v/pg-listen.svg" />
  </a>
</p>

<br />

PostgreSQL can act as a message broker: Send notifications with arbitrary payloads from one database client to others.

Works with node.js 18+, node-postgres 8, and plain JavaScript or TypeScript. Uses the Postgres [`NOTIFY`](https://www.postgresql.org/docs/10/static/sql-notify.html) statement and subscribes to notifications using [`LISTEN`](https://www.postgresql.org/docs/10/static/sql-listen.html).

### Features

&nbsp;&nbsp;&nbsp;&nbsp;📡&nbsp;&nbsp;Send and subscribe to messages

&nbsp;&nbsp;&nbsp;&nbsp;⏳&nbsp;&nbsp;Continuous connection health checks

&nbsp;&nbsp;&nbsp;&nbsp;♻️&nbsp;&nbsp;Reconnects automatically

&nbsp;&nbsp;&nbsp;&nbsp;❗️&nbsp;&nbsp;Proper error handling

&nbsp;&nbsp;&nbsp;&nbsp;👌&nbsp;&nbsp;Type-safe API

---


## This fork (Ganderworks)

A maintained fork of [andywer/pg-listen](https://github.com/andywer/pg-listen), same API, fixing the ways a long-lived listener dies quietly in production:

- **It never stops reconnecting** by default. Upstream gave up after 3s (`retryTimeout`), leaving the subscriber dead after any longer outage. Now `retryTimeout` defaults to 0 (never), and retries back off from 500ms, doubling, to 10s.
- **Half-open connections are caught.** The paranoid check's query now has a timeout (`paranoidTimeout`, default the interval or 10s, whichever is less); before, a silent connection hung the check forever and nothing ever noticed.
- **Connecting can't stall.** `connectionTimeoutMillis` defaults to 10s, and retired connections have their socket destroyed rather than waiting on a goodbye from a peer that's gone.
- **The first `connect()` retries** like a reconnect does, instead of throwing on the first failure.
- **No crash from a client it has let go of** erroring later, and no leaked client per failed attempt.
- **`"connected"` means listening** on every channel, including ones added while reconnecting, so it's the moment to catch up on what was sent while disconnected. A connection lost during that setup is replaced, not kept.
- **`close()` is final:** it stops a reconnect in flight (even a stalled one) and resolves once nothing is left running.
- **Nothing arrives for a channel after `unlisten()`**, even a notification already on the wire.
- **One query at a time** on its connection, ready for node-postgres 9 (which drops its own queue).
- Dependencies: `debug` only (`pg-format` and `typed-emitter` are gone). `dist/` is committed, so installing from git needs no build.

Install it pinned to a commit: `npm install github:Ganderworks/pg-listen#<sha>`.

Tests run against a real Postgres, with a TCP proxy that refuses, stalls, pauses or blackholes connections: `npm run test:database`, then `npm test` (or set `DATABASE_URL`).

## Installation

```sh
# using npm:
npm install pg-listen

# using yarn:
yarn add pg-listen
```


## Usage

```js
import createSubscriber from "pg-listen"
import { databaseURL } from "./config"

// Accepts the same connection config object that the "pg" package would take
const subscriber = createSubscriber({ connectionString: databaseURL })

subscriber.notifications.on("my-channel", (payload) => {
  // Payload as passed to subscriber.notify() (see below)
  console.log("Received notification in 'my-channel':", payload)
})

subscriber.events.on("error", (error) => {
  console.error("Fatal database connection error:", error)
  process.exit(1)
})

process.on("exit", () => {
  subscriber.close()
})

export async function connect () {
  await subscriber.connect()
  await subscriber.listenTo("my-channel")
}

export async function sendSampleMessage () {
  await subscriber.notify("my-channel", {
    greeting: "Hey, buddy.",
    timestamp: Date.now()
  })
}
```


## API

For details see [dist/index.d.ts](./dist/index.d.ts).


## Error & event handling

#### `instance.events.on("connected", listener: () => void)`

The `connected` event is emitted once after initially establishing the connection and later once after every successful reconnect. Reconnects happen automatically when `pg-listen` detects that the connection closed or became unresponsive. By then it is listening on every subscribed channel. Postgres doesn't keep notifications for a listener that wasn't there, so after a reconnect this is when to catch up on anything sent in between.

#### `instance.events.on("error", listener: (error: Error) => void)`

An `error` event is emitted for a notification payload that didn't parse (listening carries on), and when reconnecting gives up, which with the default options it never does: only a `retryLimit` or `retryTimeout` you set ends it. An `error` nobody listens to throws, like any EventEmitter's, so always listen.

#### `instance.events.on("notification", listener: ({ channel, payload }) => void)`

Emitted whenever a notification is received. You must have subscribed to that channel before using `instance.listenTo()` in order to receive notifications.

A more convenient way of subscribing to notifications is the `instance.notifications` event emitter.

#### `instance.events.on("reconnect", listener: (attempt: number) => void)`

Emitted when a connection issue has been detected and an attempt to re-connect to the database is started.

#### `instance.notifications.on(channelName: string, listener: (payload: any) => void)`

The convenient way of subscribing to notifications. Don't forget to call `.listenTo(channelName)` to subscribe the Postgres client to this channel in order to receive notifications.


## Why another package?

In one sentence: Because none of the existing packages was working reliably in production.

Using the `NOTIFY` and `LISTEN` features is not trivial using [`node-postgres` (`pg`)](https://www.npmjs.com/package/pg) directly, since you cannot use connection pools and even distinct client connections also tend to time out.

There are already a few packages out there, like `pg-pubsub`, but neither of them seems to work reliably. Errors are being swallowed, the code is hard to reason about, there is no type-safety, ...

This package aims to fix those shortcomings. Postgres LISTEN & NOTIFY in node that finally works.


## Debugging

Set the `DEBUG` environment variable to `pg-listen:*` to enable debug logging.


## License

MIT
