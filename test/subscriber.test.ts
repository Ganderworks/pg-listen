import { afterEach, beforeAll, afterAll, describe, expect, it, vi } from "vitest"
import pg from "pg"

import createPostgresSubscriber, { Options, PgParsedNotification, Subscriber } from "../src/index"
import { admin, backends, DATABASE_URL, delay, proxy, terminate, until } from "./helpers"

let db: pg.Client
let n = 0
const opened: Subscriber[] = []
const proxies: Awaited<ReturnType<typeof proxy>>[] = []

beforeAll(async () => {
  db = await admin()
})
afterAll(async () => {
  await db.end()
})
afterEach(async () => {
  vi.restoreAllMocks()
  await Promise.all(opened.splice(0).map(s => s.close()))
  await Promise.all(proxies.splice(0).map(p => p.close()))
})

/** A subscriber that records what it emits, under its own application_name. */
function subscriber(options: Options = {}, config: pg.ClientConfig = {}) {
  const name = `pg-listen-test-${process.pid}-${++n}`
  const hub = createPostgresSubscriber({ connectionString: DATABASE_URL, ...config, application_name: name }, options)
  opened.push(hub)
  const seen = {
    name,
    hub,
    connected: 0,
    reconnects: [] as number[],
    errors: [] as Error[],
    notifications: [] as PgParsedNotification[],
    payloads: (channel = "test") => seen.notifications.filter(x => x.channel === channel).map(x => x.payload)
  }
  hub.events.on("connected", () => seen.connected++)
  hub.events.on("reconnect", attempt => seen.reconnects.push(attempt))
  hub.events.on("error", error => seen.errors.push(error))
  hub.events.on("notification", x => seen.notifications.push(x))
  return seen
}

async function through() {
  const p = await proxy()
  proxies.push(p)
  return p
}

/** Every pg.Client that connects from here on, in order. */
function trackClients(): pg.Client[] {
  const connect = pg.Client.prototype.connect
  const clients: pg.Client[] = []
  vi.spyOn(pg.Client.prototype, "connect").mockImplementation(function (this: pg.Client, ...args: any[]) {
    clients.push(this)
    return (connect as any).apply(this, args)
  })
  return clients
}

/** Its socket is gone, not half-closed and waiting. */
const destroyed = (client: pg.Client) => Boolean((client as any).connection.stream.destroyed)

const notify = (channel: string, payload: unknown) =>
  db.query("select pg_notify($1, $2)", [channel, JSON.stringify(payload)])

describe("listening", () => {
  it("connects, listens and gets notified", async () => {
    type ChannelEvents = { test: { hello: string }; test2: string }
    const s = subscriber()
    const hub = s.hub as unknown as Subscriber<ChannelEvents>
    const received: unknown[] = []
    hub.notifications.on("test", payload => received.push(payload))
    await hub.connect()
    await hub.listenTo("test")

    await hub.notify("test", { hello: "world" })
    await hub.notify("test2", "not subscribed")
    await until("the notification", () => received.length === 1)
    await delay(100)

    expect(hub.getSubscribedChannels()).toEqual(["test"])
    expect(s.notifications).toEqual([{ channel: "test", payload: { hello: "world" }, processId: expect.any(Number) }])
    expect(received).toEqual([{ hello: "world" }])
    expect(s.connected).toBe(1)
    expect(s.reconnects).toEqual([])
  })

  it("handles a notification without payload", async () => {
    const s = subscriber()
    await s.hub.connect()
    await s.hub.listenTo("test")
    await s.hub.notify("test")
    await until("the notification", () => s.notifications.length === 1)
    expect(s.notifications[0]!.payload).toBeUndefined()
  })

  it("uses a custom parse function", async () => {
    const s = subscriber({ parse: base64 => Buffer.from(base64, "base64").toString("utf8") })
    await s.hub.connect()
    await s.hub.listenTo("test")
    await db.query(`NOTIFY test, '${Buffer.from("I am a payload.").toString("base64")}'`)
    await until("the notification", () => s.notifications.length === 1)
    expect(s.payloads()).toEqual(["I am a payload."])
  })

  it("channels listened to before connect() are listened to on connect", async () => {
    const s = subscriber()
    s.hub.listenTo("early")
    await s.hub.connect()
    await notify("early", 1)
    await until("the notification", () => s.payloads("early").length === 1)
  })

  it("quotes channel names", async () => {
    const s = subscriber()
    await s.hub.connect()
    const odd = `chat"; drop table x; --`
    await s.hub.listenTo(odd)
    await s.hub.notify(odd, "ok")
    await until("the notification", () => s.payloads(odd).length === 1)
  })

  it("unlisten stops a channel; unlistenAll stops them all", async () => {
    const s = subscriber()
    await s.hub.connect()
    await s.hub.listenTo("a")
    await s.hub.listenTo("b")
    await s.hub.unlisten("a")
    await notify("a", 1)
    await notify("b", 1)
    await until("b", () => s.payloads("b").length === 1)
    await s.hub.unlistenAll()
    await notify("b", 2)
    await delay(200)
    expect(s.notifications.map(x => [x.channel, x.payload])).toEqual([["b", 1]])
  })

  it("delivers a burst in commit order, and a near-8000-byte payload", async () => {
    const s = subscriber()
    await s.hub.connect()
    await s.hub.listenTo("test")
    // One transaction per notification.
    for (let i = 0; i < 1000; i++) await notify("test", i)
    await until("all 1000", () => s.payloads().length === 1000)
    expect(s.payloads()).toEqual(Array.from({ length: 1000 }, (_, i) => i))

    const big = "x".repeat(7990)
    await notify("big", big)
    await s.hub.listenTo("big")
    await notify("big", big)
    await until("the big one", () => s.payloads("big").length === 1)
    expect(s.payloads("big")[0]).toHaveLength(7990)
  })

  it("a payload that doesn't parse is an error, and listening goes on", async () => {
    const s = subscriber()
    await s.hub.connect()
    await s.hub.listenTo("test")
    await db.query(`NOTIFY test, 'not json'`)
    await notify("test", "fine")
    await until("the good one", () => s.payloads().length === 1)
    expect(s.errors.map(e => e.message)).toEqual([expect.stringContaining("Error parsing PostgreSQL notification payload")])
    expect(s.payloads()).toEqual(["fine"])
  })
  it("never overlaps queries on its connection (node-postgres 9 won't queue them)", async () => {
    const query = pg.Client.prototype.query
    const s = subscriber({ paranoidChecking: 5, paranoidTimeout: 5000 })
    let overlaps = 0
    vi.spyOn(pg.Client.prototype, "query").mockImplementation(function (this: pg.Client, ...args: any[]) {
      const self = this as any
      if (self.connectionParameters?.application_name === s.name && (self._activeQuery || self._queryQueue.length)) {
        overlaps++
      }
      return (query as any).apply(this, args)
    })
    for (let i = 0; i < 20; i++) s.hub.listenTo(`c${i}`)
    await s.hub.connect()
    await Promise.all(Array.from({ length: 20 }, (_, i) => [s.hub.listenTo(`d${i}`), s.hub.notify(`c${i}`, i)]).flat())
    await until("the notifications", () => s.notifications.length === 20)
    await delay(100)
    expect(overlaps).toBe(0)
  })
})

describe("losing the connection", () => {
  it("reconnects after its backend is terminated, and listens again", async () => {
    const s = subscriber({ retryInterval: 50 })
    await s.hub.connect()
    await s.hub.listenTo("test")

    expect(await terminate(db, s.name)).toBe(1)
    await until("connected again", () => s.connected === 2)
    await notify("test", { hello: "world" })
    await until("the notification", () => s.payloads().length === 1)

    expect(s.hub.getSubscribedChannels()).toEqual(["test"])
    expect(s.reconnects).toEqual([1])
    expect(s.errors).toEqual([])
    expect(await backends(db, s.name)).toBe(1)
  })

  it('"connected" means listening on every channel: a NOTIFY sent from its handler arrives', async () => {
    const s = subscriber({ retryInterval: 50 })
    await s.hub.connect()
    for (let i = 0; i < 50; i++) await s.hub.listenTo(`c${i}`)
    s.hub.events.on("connected", () => void notify("c49", "caught up"))
    await terminate(db, s.name)
    await until("the notification", () => s.payloads("c49").length === 1)
  })

  it("a connection lost just as it finished subscribing is replaced", async () => {
    const s = subscriber({ retryInterval: 20, paranoidChecking: false })
    await s.hub.connect()
    await s.hub.listenTo("test")
    // The next LISTEN succeeds, and then its connection dies: after setup's
    // last query, before it says "connected".
    const query = pg.Client.prototype.query
    let armed = true
    vi.spyOn(pg.Client.prototype, "query").mockImplementation(function (this: pg.Client, ...args: any[]) {
      const result = (query as any).apply(this, args)
      const mine = (this as any).connectionParameters?.application_name === s.name
      if (!armed || !mine || !String(args[0]).startsWith("LISTEN")) return result
      armed = false
      return result.then((r: unknown) => {
        this.emit("error", new Error("lost"))
        ;(this as any).connection.stream.destroy()
        return r
      })
    })
    await terminate(db, s.name)
    await until("connected again", () => s.connected === 2)
    expect(armed).toBe(false)
    expect(s.reconnects.length).toBeGreaterThanOrEqual(2)
    await notify("test", "ok")
    await until("the notification", () => s.payloads().length === 1)
    await until("one backend", async () => (await backends(db, s.name)) === 1)
  })

  it("a client it has let go of can error, more than once, without crashing", async () => {
    const clients = trackClients()
    const s = subscriber({ retryInterval: 20 })
    await s.hub.connect()
    await terminate(db, s.name)
    await until("connected again", () => s.connected === 2)
    expect(clients).toHaveLength(2)
    expect(() => {
      clients[0]!.emit("error", new Error("late"))
      clients[0]!.emit("error", new Error("later"))
    }).not.toThrow()
    expect(s.errors).toEqual([])
  })

  it("nothing arrives for a channel after unlisten(), even if already on the wire", async () => {
    const p = await through()
    const s = subscriber({}, { connectionString: p.url })
    await s.hub.connect()
    await s.hub.listenTo("a")
    await s.hub.listenTo("b")
    p.pause()
    await notify("a", "on the wire")
    await notify("b", "on the wire")
    void s.hub.unlisten("a")
    p.resume()
    await until("b", () => s.payloads("b").length === 1)
    await delay(100)
    expect(s.payloads("a")).toEqual([])
  })

  it("notifications sent while disconnected are gone (callers must catch up)", async () => {
    const p = await through()
    const s = subscriber({ retryInterval: 200 }, { connectionString: p.url })
    await s.hub.connect()
    await s.hub.listenTo("test")
    p.refuse()
    p.kill()
    await until("a reconnect attempt", () => s.reconnects.length > 0)
    await notify("test", "missed")
    p.pass()
    await until("connected again", () => s.connected === 2)
    await notify("test", "after")
    await until("the notification", () => s.payloads().length === 1)
    expect(s.payloads()).toEqual(["after"])
  })

  it("keeps trying through an outage longer than upstream's 3s timeout", async () => {
    const p = await through()
    const s = subscriber({ retryInterval: 250 }, { connectionString: p.url })
    await s.hub.connect()
    await s.hub.listenTo("test")

    p.refuse()
    p.kill()
    await delay(4000)
    expect(s.reconnects.length).toBeGreaterThan(5)
    expect(s.connected).toBe(1)
    p.pass()

    await until("connected again", () => s.connected === 2)
    await notify("test", "back")
    await until("the notification", () => s.payloads().length === 1)
    expect(s.errors).toEqual([])
  })

  it("backs off by default: 500ms, doubling", async () => {
    const p = await through()
    const s = subscriber({}, { connectionString: p.url })
    await s.hub.connect()
    p.refuse()
    const start = Date.now()
    p.kill()
    // Attempts at about 0, 0.5, 1.5, 3.5s.
    await delay(3000)
    expect(s.reconnects).toEqual([1, 2, 3])
    p.pass()
    await until("connected again", () => s.connected === 2)
    expect(Date.now() - start).toBeGreaterThan(3400)
  })

  it("catches a half-open connection with the paranoid check, and drops its socket", async () => {
    const clients = trackClients()
    const p = await through()
    const s = subscriber({ paranoidChecking: 300, paranoidTimeout: 300, retryInterval: 50 }, { connectionString: p.url })
    await s.hub.connect()
    await s.hub.listenTo("test")
    expect(p.open).toBe(1)

    // The connection stays open but goes silent: no error, no end. Only the
    // check's timeout can tell.
    p.blackhole()
    await notify("test", "into the void")
    await until("connected again", () => s.connected === 2, 3000)
    await notify("test", "after")
    await until("the notification", () => s.payloads().length === 1)
    expect(s.payloads()).toEqual(["after"])

    // The silent socket was destroyed: end() alone would leave it waiting on
    // a goodbye from a peer that's gone.
    await until("the old socket to be destroyed", () => destroyed(clients[0]!), 3000)
    expect(p.accepted).toBe(2)
    expect(s.errors).toEqual([])
  })

  it("doesn't leak connections over many reconnects", async () => {
    const s = subscriber({ retryInterval: 20 })
    await s.hub.connect()
    await s.hub.listenTo("test")
    for (let i = 1; i <= 10; i++) {
      await terminate(db, s.name)
      await until(`reconnect ${i}`, () => s.connected === i + 1)
    }
    expect(await backends(db, s.name)).toBe(1)
    await notify("test", "still here")
    await until("the notification", () => s.payloads().length === 1)
  })

  it("listenTo and unlisten during an outage apply on reconnect", async () => {
    const p = await through()
    const s = subscriber({ retryInterval: 100 }, { connectionString: p.url })
    await s.hub.connect()
    await s.hub.listenTo("old")
    p.refuse()
    p.kill()
    await until("a reconnect attempt", () => s.reconnects.length > 0)

    expect(s.hub.listenTo("new")).toBeUndefined()
    expect(s.hub.unlisten("old")).toBeUndefined()
    await expect(s.hub.notify("new", 1)).rejects.toThrow("Not connected")

    p.pass()
    await until("connected again", () => s.connected === 2)
    await notify("old", 1)
    await notify("new", 2)
    await until("the notification", () => s.payloads("new").length === 1)
    await delay(100)
    expect(s.notifications.map(x => x.channel)).toEqual(["new"])
  })

  it("survives the connection dying again mid-reconnect", async () => {
    const s = subscriber({ retryInterval: 30 })
    await s.hub.connect()
    await s.hub.listenTo("test")
    // Keep killing whatever it connects with, for a second.
    const stop = Date.now() + 1000
    while (Date.now() < stop) {
      await terminate(db, s.name)
      await delay(15)
    }
    await until("settled", async () => (await backends(db, s.name)) === 1)
    await delay(300)
    await notify("test", "settled")
    await until("the notification", () => s.payloads().length === 1, 5000)
    expect(await backends(db, s.name)).toBe(1)
    expect(s.errors).toEqual([])
  })
})

describe("connecting", () => {
  it("an attempt that connects but hears nothing times out and retries", async () => {
    const p = await through()
    p.stall()
    const s = subscriber({ retryInterval: 50 }, { connectionString: p.url, connectionTimeoutMillis: 300 })
    const connecting = s.hub.connect()
    await delay(1000)
    expect(s.connected).toBe(0)
    expect(s.reconnects.length).toBeGreaterThanOrEqual(2)
    p.pass()
    await connecting
    expect(s.connected).toBe(1)
  })

  it("times out a silent connect by default (10s)", async () => {
    const p = await through()
    p.stall()
    const s = subscriber({ retryInterval: 50 }, { connectionString: p.url })
    const start = Date.now()
    const connecting = s.hub.connect()
    await delay(9000)
    expect(s.reconnects).toEqual([])
    // The next attempt gets through.
    p.pass()
    await connecting
    expect(s.reconnects).toEqual([1])
    expect(Date.now() - start).toBeGreaterThanOrEqual(10000)
  }, 20000)

  it("the first connect waits for an unreachable database", async () => {
    const p = await through()
    p.refuse()
    const s = subscriber({ retryInterval: 100 }, { connectionString: p.url })
    const connecting = s.hub.connect()
    await delay(600)
    expect(s.connected).toBe(0)
    p.pass()
    await connecting
    expect(s.connected).toBe(1)
    expect(s.reconnects.length).toBeGreaterThan(2)
    expect(s.reconnects[0]).toBe(1)
  })

  it("retryLimit gives up, rejecting connect()", async () => {
    const p = await through()
    p.refuse()
    const s = subscriber({ retryInterval: 20, retryLimit: 3 }, { connectionString: p.url })
    await expect(s.hub.connect()).rejects.toThrow("failed after 3 attempts")
    expect(s.reconnects).toEqual([1, 2])
  })

  it("retryLimit gives up after a loss with an error event", async () => {
    const p = await through()
    const s = subscriber({ retryInterval: 20, retryLimit: 3 }, { connectionString: p.url })
    await s.hub.connect()
    p.refuse()
    p.kill()
    await until("the error", () => s.errors.length === 1)
    expect(s.errors[0]!.message).toContain("failed after 3 attempts")
    expect(s.reconnects).toEqual([1, 2, 3])
  })

  it("retryTimeout, if set, still gives up", async () => {
    const p = await through()
    const s = subscriber({ retryInterval: 50, retryTimeout: 300 }, { connectionString: p.url })
    await s.hub.connect()
    p.refuse()
    p.kill()
    await until("the error", () => s.errors.length === 1)
    expect(s.errors[0]!.message).toContain("300ms timeout")
  })
})

describe("closing", () => {
  it("close() during an outage stops reconnecting, promptly", async () => {
    const p = await through()
    const s = subscriber({ retryInterval: 5000 }, { connectionString: p.url })
    await s.hub.connect()
    p.refuse()
    p.kill()
    await until("a reconnect attempt", () => s.reconnects.length === 1)
    const start = Date.now()
    await s.hub.close()
    p.pass()
    await delay(500)
    expect(Date.now() - start).toBeLessThan(1000)
    expect(p.accepted).toBe(1)
    expect(s.connected).toBe(1)
    expect(s.errors).toEqual([])
  })

  it("close() hangs up on a connect that's stalled, promptly", async () => {
    const clients = trackClients()
    const p = await through()
    p.stall()
    const s = subscriber({}, { connectionString: p.url })
    const connecting = s.hub.connect()
    await until("the attempt", () => p.accepted === 1)
    const start = Date.now()
    await s.hub.close()
    expect(Date.now() - start).toBeLessThan(500)
    // Hung up by the time close() resolves, not some time after.
    expect((clients[0] as any)._ending).toBe(true)
    await expect(connecting).rejects.toThrow("closed")
    await until("its socket to be destroyed", () => destroyed(clients[0]!), 3000)
  })

  it("close() from a reconnect listener: nothing connects after it", async () => {
    const clients = trackClients()
    const s = subscriber({ retryInterval: 20 })
    await s.hub.connect()
    let closed: Promise<void> | undefined
    s.hub.events.on("reconnect", () => (closed ??= s.hub.close()))
    await terminate(db, s.name)
    await until("close() to be called", () => closed !== undefined)
    await closed
    // It stops before making the attempt it announced.
    expect(clients).toHaveLength(1)
    await delay(200)
    expect(s.connected).toBe(1)
    expect(await backends(db, s.name)).toBe(0)
  })

  it("close() during the first connect rejects it and leaves nothing open", async () => {
    const p = await through()
    p.refuse()
    const s = subscriber({ retryInterval: 50 }, { connectionString: p.url })
    const connecting = s.hub.connect()
    await until("an attempt", () => s.reconnects.length > 0)
    await s.hub.close()
    await expect(connecting).rejects.toThrow("closed")
    p.pass()
    await delay(300)
    expect(await backends(db, s.name)).toBe(0)
  })

  it("close() hangs up, and a terminated backend after close isn't reconnected", async () => {
    const s = subscriber({ retryInterval: 20 })
    await s.hub.connect()
    await s.hub.close()
    await until("hung up", async () => (await backends(db, s.name)) === 0)
    await delay(200)
    expect(s.reconnects).toEqual([])
    expect(await backends(db, s.name)).toBe(0)
  })
})
