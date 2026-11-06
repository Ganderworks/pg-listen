import net from "net"
import pg from "pg"

/** The test database. `npm run test:database` starts one on 5432. */
export const DATABASE_URL = process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:5432/postgres"
const target = new URL(DATABASE_URL)

export const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

export async function until(what: string, ok: () => boolean | Promise<boolean>, ms = 10000): Promise<number> {
  const start = Date.now()
  while (!(await ok())) {
    if (Date.now() - start > ms) throw new Error(`timed out waiting for ${what}`)
    await delay(20)
  }
  return Date.now() - start
}

/** A plain client to send NOTIFYs and kill backends with. */
export async function admin(): Promise<pg.Client> {
  const client = new pg.Client({ connectionString: DATABASE_URL })
  await client.connect()
  return client
}

/** Live backends with this application_name. */
export async function backends(db: pg.Client, applicationName: string): Promise<number> {
  const { rows } = await db.query("select count(*)::int as n from pg_stat_activity where application_name = $1", [
    applicationName
  ])
  return rows[0].n
}

export async function terminate(db: pg.Client, applicationName: string): Promise<number> {
  const { rows } = await db.query(
    "select count(*)::int as n from (select pg_terminate_backend(pid) from pg_stat_activity where application_name = $1) t",
    [applicationName]
  )
  return rows[0].n
}

/**
 * A TCP proxy in front of the database, to break the network in ways
 * pg_terminate_backend can't:
 *  - refuse: new connections are hung up on at once (the database is down)
 *  - stall: new connections are accepted and then hear nothing
 *  - blackhole: open connections stop carrying data and never close, not
 *    even when asked to: the half-open connection a dead NAT box or a
 *    failed-over host leaves you
 *  - pause/resume: open connections hold their data, then deliver it
 */
export async function proxy() {
  let mode: "pass" | "refuse" | "stall" = "pass"
  type Pair = { client: net.Socket; upstream: net.Socket; frozen: boolean; held: [net.Socket, Buffer][] | null }
  const pairs = new Set<Pair>()
  let accepted = 0

  // allowHalfOpen: a FIN from one side is passed on (or, frozen, swallowed)
  // by us, rather than answered automatically by node.
  const server = net.createServer({ allowHalfOpen: true }, client => {
    if (mode === "refuse") {
      client.destroy()
      return
    }
    accepted++
    const upstream = net.connect({ port: Number(target.port || 5432), host: target.hostname, allowHalfOpen: true })
    const pair: Pair = { client, upstream, frozen: mode === "stall", held: null }
    pairs.add(pair)
    const forward = (to: net.Socket) => (data: Buffer) => {
      if (pair.frozen) return
      if (pair.held) pair.held.push([to, data])
      else to.write(data)
    }
    client.on("data", forward(upstream))
    upstream.on("data", forward(client))
    client.on("end", () => !pair.frozen && upstream.end())
    upstream.on("end", () => !pair.frozen && client.end())
    const drop = () => {
      client.destroy()
      upstream.destroy()
      pairs.delete(pair)
    }
    for (const socket of [client, upstream]) {
      socket.on("close", drop)
      socket.on("error", drop)
    }
  })
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve))
  const port = (server.address() as net.AddressInfo).port
  const url = new URL(DATABASE_URL)
  url.hostname = "127.0.0.1"
  url.port = String(port)

  return {
    url: url.toString(),
    /** Connections made through the proxy so far. */
    get accepted() {
      return accepted
    },
    /** Connections open through the proxy now. */
    get open() {
      return pairs.size
    },
    refuse() {
      mode = "refuse"
    },
    stall() {
      mode = "stall"
    },
    pass() {
      mode = "pass"
    },
    /** Cut every open connection. */
    kill() {
      for (const pair of pairs) {
        pair.client.destroy()
        pair.upstream.destroy()
      }
    },
    /** Open connections go silent; new ones still work. */
    blackhole() {
      for (const pair of pairs) pair.frozen = true
    },
    pause() {
      for (const pair of pairs) pair.held ??= []
    },
    resume() {
      for (const pair of pairs) {
        const held = pair.held ?? []
        pair.held = null
        for (const [to, data] of held) to.write(data)
      }
    },
    async close() {
      this.kill()
      await new Promise(resolve => server.close(resolve))
    }
  }
}
