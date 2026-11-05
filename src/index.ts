import createDebugLogger from "debug"
import { EventEmitter } from "events"
import pg from "pg"

const connectionLogger = createDebugLogger("pg-listen:connection")
const notificationLogger = createDebugLogger("pg-listen:notification")
const paranoidLogger = createDebugLogger("pg-listen:paranoid")
const subscriptionLogger = createDebugLogger("pg-listen:subscription")

export interface PgParsedNotification {
  processId: number
  channel: string
  payload?: any
}

interface PgListenEvents {
  /** Connected and listening on every subscribed channel. After a reconnect,
   *  this is the moment to catch up on whatever was sent while disconnected:
   *  Postgres doesn't keep notifications for a listener that wasn't there. */
  connected: () => void
  /** A payload that didn't parse, or reconnecting gave up (retryLimit,
   *  retryTimeout). Like any EventEmitter, an "error" nobody listens to throws. */
  error: (error: Error) => void
  notification: (notification: PgParsedNotification) => void
  /** A connection attempt after the first: `attempt` counts from 1. */
  reconnect: (attempt: number) => void
}

type Arguments<T> = [T] extends [(...args: infer U) => any] ? U : [T] extends [void] ? [] : [T]

/** An EventEmitter whose event names and listener arguments are typed. */
export interface TypedEventEmitter<Events> {
  addListener<E extends keyof Events>(event: E, listener: Events[E]): this
  on<E extends keyof Events>(event: E, listener: Events[E]): this
  once<E extends keyof Events>(event: E, listener: Events[E]): this
  prependListener<E extends keyof Events>(event: E, listener: Events[E]): this
  prependOnceListener<E extends keyof Events>(event: E, listener: Events[E]): this
  off<E extends keyof Events>(event: E, listener: Events[E]): this
  removeAllListeners<E extends keyof Events>(event?: E): this
  removeListener<E extends keyof Events>(event: E, listener: Events[E]): this
  emit<E extends keyof Events>(event: E, ...args: Arguments<Events[E]>): boolean
  eventNames(): (keyof Events)[]
  listeners<E extends keyof Events>(event: E): Function[]
  listenerCount<E extends keyof Events>(event: E): number
  getMaxListeners(): number
  setMaxListeners(maxListeners: number): this
}

type EventsToEmitterHandlers<Events extends Record<string, any>> = {
  [channelName in keyof Events]: (payload: Events[channelName]) => void
}

export interface Options {
  /**
   * Using native PG client? Defaults to false.
   */
  native?: boolean

  /**
   * Interval in ms to run a trivial query on the DB to see if
   * the database connection still works.
   * Defaults to 30s.
   */
  paranoidChecking?: number | false

  /**
   * How long the paranoid check's query may take before the connection
   * counts as dead. A half-open TCP connection (the server or a NAT box
   * dropped it without telling us) never errors on its own; this is what
   * catches it. Defaults to the paranoid interval, at most 10s.
   */
  paranoidTimeout?: number

  /**
   * How much time to wait between connection attempts (if failed).
   * Can also be a callback taking the attempt (from 0) and returning a
   * delay in milliseconds. Defaults to 500ms, doubling up to 10s.
   */
  retryInterval?: number | ((attempt: number) => number)

  /**
   * How many connection attempts before giving up. Defaults to no limit.
   */
  retryLimit?: number

  /**
   * Time in ms after which to stop retrying and just fail. Defaults to 0:
   * never stop. (Upstream stopped after 3s, leaving the subscriber dead for
   * good after any outage longer than that.)
   */
  retryTimeout?: number

  /**
   * Custom function to control how the payload data is parsed.
   * Use together with the `serialize` option. Defaults to `JSON.parse`.
   */
  parse?: (serialized: string) => any

  /**
   * Custom function to control how the payload data is stringified on `.notify()`.
   * Use together with the `parse` option. Defaults to `JSON.stringify`.
   */
  serialize?: (data: any) => string
}

export interface Subscriber<Events extends Record<string, any> = { [channel: string]: any }> {
  /** Emits events: "connected", "error", "notification" & "reconnect" */
  events: TypedEventEmitter<PgListenEvents>
  /** For convenience: Subscribe to distinct notifications here, event name = channel name */
  notifications: TypedEventEmitter<EventsToEmitterHandlers<Events>>
  /** Connect, retrying (per the retry options) until it does. Resolves once
   *  listening on every channel subscribed so far. */
  connect(): Promise<void>
  /** Hang up and stop reconnecting. Resolves once nothing is left running. */
  close(): Promise<void>
  getSubscribedChannels(): string[]
  /** Subscribe. While disconnected, the channel is listened to on reconnect. */
  listenTo(channelName: string): Promise<pg.QueryResult> | undefined
  notify<EventName extends keyof Events>(
    channelName: any extends Events[EventName] ? EventName : void extends Events[EventName] ? never : EventName,
    payload: Events[EventName] extends void ? never : Events[EventName]
  ): Promise<pg.QueryResult>
  notify<EventName extends keyof Events>(
    channelName: void extends Events[EventName] ? EventName : never
  ): Promise<pg.QueryResult>
  unlisten(channelName: string): Promise<pg.QueryResult> | undefined
  unlistenAll(): Promise<pg.QueryResult> | undefined
}

/** Thrown inside the connection loop when close() was called: stop quietly. */
class Closed extends Error {
  constructor() {
    super("The subscriber was closed")
  }
}
/** Thrown when retryLimit or retryTimeout ran out: give up and say so. */
class GaveUp extends Error {}

const noop = () => undefined

function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

/** Queries on a client, one at a time: node-postgres 9 drops its own queue
 *  for overlapping queries on one client. */
const queues = new WeakMap<pg.Client, Promise<unknown>>()
function query(client: pg.Client, text: string, values?: unknown[]): Promise<pg.QueryResult> {
  const run = (queues.get(client) ?? Promise.resolve()).then(() => client.query(text, values))
  queues.set(client, run.catch(noop))
  return run
}

/** Hang up on a client we're done with. end() says goodbye and waits for
 *  the server's, which a stalled or half-open connection never sends.
 *  node-postgres destroys the socket itself only if a query is running, so
 *  it's destroyed here shortly after (there's no public way to do that). */
function retire(client: pg.Client) {
  client.end().catch(noop)
  const stream: { destroy?: () => void } | undefined = (client as any).connection?.stream
  setTimeout(() => stream?.destroy?.(), 1000).unref()
}

function createPostgresSubscriber<Events extends Record<string, any> = { [channel: string]: any }>(
  connectionConfig?: pg.ClientConfig,
  options: Options = {}
): Subscriber<Events> {
  const {
    paranoidChecking = 30000,
    parse = JSON.parse,
    serialize = JSON.stringify,
    retryInterval = (attempt: number) => Math.min(500 * 2 ** attempt, 10000),
    retryLimit = Infinity,
    retryTimeout = 0
  } = options
  const paranoidTimeout = options.paranoidTimeout ?? Math.min(paranoidChecking || 10000, 10000)
  const getRetryInterval = typeof retryInterval === "function" ? retryInterval : () => retryInterval

  // A connect that never hears back (a host that accepts and goes silent)
  // would otherwise stall reconnecting forever.
  const config: pg.ClientConfig = { connectionTimeoutMillis: 10000, ...connectionConfig, keepAlive: true }
  const Client = options.native && pg.native ? pg.native.Client : pg.Client

  const emitter = new EventEmitter() as TypedEventEmitter<PgListenEvents>
  emitter.setMaxListeners(0) // unlimited listeners

  const notificationsEmitter = new EventEmitter() as TypedEventEmitter<EventsToEmitterHandlers<Events>>
  notificationsEmitter.setMaxListeners(0) // unlimited listeners

  emitter.on("notification", (notification: PgParsedNotification) => {
    (notificationsEmitter as unknown as EventEmitter).emit(notification.channel, notification.payload)
  })

  let client: pg.Client | null = null
  let closing = false
  let reinitializing = false
  let subscribedChannels: Set<string> = new Set()
  /** Clients whose connection was lost, possibly while we were still setting them up. */
  const lost = new WeakSet<pg.Client>()

  let stopWatching: () => void = noop

  // A retry wait that close() can cut short.
  let wake: () => void = noop
  const sleep = (ms: number) =>
    new Promise<void>(resolve => {
      const timer = setTimeout(resolve, ms)
      wake = () => {
        clearTimeout(timer)
        resolve()
      }
    })

  /** Rejects the connection attempt in flight, for close(). */
  let abortAttempt: (error: Error) => void = noop

  /** Open a connection, retrying per the options. */
  const open = async (onAttempt: (attempt: number) => void): Promise<pg.Client> => {
    const startTime = Date.now()
    for (let attempt = 1; ; attempt++) {
      if (closing) throw new Closed()
      onAttempt(attempt)
      // A listener may have closed us.
      if (closing) throw new Closed()
      connectionLogger(`PostgreSQL connection attempt #${attempt}...`)

      const next = new Client(config)
      // Permanent: a client can error more than once (or after we've given up
      // on it), and an "error" with no listener would crash the process.
      next.on("error", (error: Error) => connectionLogger("DB client error:", error))
      try {
        // Ending a client mid-connect never settles its connect() (node-postgres
        // drops the callback once it's ending), so close() rejects this instead.
        const aborted = new Promise<never>((_, reject) => (abortAttempt = reject))
        await Promise.race([next.connect(), aborted])
      } catch (error) {
        retire(next)
        if (closing) throw new Closed()
        connectionLogger(`PostgreSQL connection attempt #${attempt} failed:`, error)
        const message = error instanceof Error ? error.message : String(error)
        if (attempt >= retryLimit) {
          throw new GaveUp(`Connecting to PostgreSQL failed after ${attempt} attempts: ${message}`)
        }
        if (retryTimeout && Date.now() - startTime > retryTimeout) {
          throw new GaveUp(`Stopping PostgreSQL reconnection attempts after ${retryTimeout}ms timeout has been reached: ${message}`)
        }
        await sleep(getRetryInterval(attempt - 1))
        continue
      } finally {
        abortAttempt = noop
      }
      if (closing) {
        retire(next)
        throw new Closed()
      }
      connectionLogger("PostgreSQL connection succeeded")
      return next
    }
  }

  /** Wire up a fresh client: forward its notifications, notice when it's
   *  lost, and LISTEN on every subscribed channel (including ones added
   *  while this ran). */
  const start = async (next: pg.Client) => {
    client = next

    const onNotification = (notification: pg.Notification) => {
      // Not subscribed (any more): e.g. unlistened while we were reconnecting.
      if (!subscribedChannels.has(notification.channel)) return
      notificationLogger(`Received PostgreSQL notification on "${notification.channel}":`, notification.payload)

      let payload
      try {
        payload = notification.payload ? parse(notification.payload) : undefined
      } catch (error: any) {
        error.message = `Error parsing PostgreSQL notification payload: ${error.message}`
        emitter.emit("error", error)
        return
      }
      emitter.emit("notification", {
        processId: notification.processId,
        channel: notification.channel,
        payload
      })
    }
    const onLost = (why: string) => () => {
      if (lost.has(next)) return
      lost.add(next)
      connectionLogger(`DB client ${why}`)
      if (client === next) void reinitialize()
    }
    const onError = onLost("errored")
    const onEnd = onLost("connection ended")
    next.on("notification", onNotification)
    next.on("error", onError)
    next.on("end", onEnd)

    let interval: NodeJS.Timeout | undefined
    if (paranoidChecking) {
      let checking = false
      interval = setInterval(async () => {
        if (checking) return
        checking = true
        try {
          await withTimeout(query(next, "SELECT 1"), paranoidTimeout, "Paranoid connection check")
          paranoidLogger("Paranoid connection check ok")
        } catch (error) {
          paranoidLogger("Paranoid connection check failed:", error)
          onLost("failed the paranoid check")()
        } finally {
          checking = false
        }
      }, paranoidChecking)
    }
    stopWatching = () => {
      clearInterval(interval)
      next.removeListener("notification", onNotification)
      next.removeListener("error", onError)
      next.removeListener("end", onEnd)
    }

    const listening = new Set<string>()
    for (;;) {
      const missing = Array.from(subscribedChannels).filter(channel => !listening.has(channel))
      if (missing.length === 0) break
      subscriptionLogger(`Subscribing to channels: ${missing.join(", ")}`)
      for (const channel of missing) await query(next, `LISTEN ${next.escapeIdentifier(channel)}`)
      missing.forEach(channel => listening.add(channel))
    }
    if (lost.has(next)) throw new Error("Connection lost while subscribing")
  }

  /** Connect and listen, retrying until it works, close() is called or the
   *  retry options give up. */
  const establish = async (onAttempt: (attempt: number) => void) => {
    for (;;) {
      const next = await open(onAttempt)
      try {
        await start(next)
        if (closing) throw new Closed()
        emitter.emit("connected")
        return
      } catch (error) {
        connectionLogger("Setting up the connection failed, reconnecting:", error)
        stopWatching()
        stopWatching = noop
        retire(next)
        if (closing) throw new Closed()
      }
    }
  }

  /** The connect or reconnect in progress, for close() to wait out. */
  let running: Promise<unknown> = Promise.resolve()

  // Handles its own errors: emits "error" if it gives up.
  const reinitialize = () => {
    if (reinitializing || closing) return
    reinitializing = true
    // Started a tick later, so `running` is this loop before any of its
    // events fire: a close() from a "reconnect" listener waits for it.
    running = Promise.resolve().then(recover)
  }
  const recover = async () => {
    try {
      stopWatching()
      stopWatching = noop
      if (client) retire(client)
      client = null

      // Every attempt here is a reconnect, the first one included.
      let attempts = 0
      await establish(() => emitter.emit("reconnect", ++attempts))
    } catch (error: any) {
      if (error instanceof Closed) return
      error.message = `Re-initializing the PostgreSQL notification client after connection loss failed: ${error.message}`
      connectionLogger(error.stack || error)
      emitter.emit("error", error)
    } finally {
      reinitializing = false
    }
  }

  /** The client to send a query on now, or null while (re)connecting. */
  const live = () => (reinitializing ? null : client)

  return {
    /** Emits events: "connected", "error", "notification" & "reconnect" */
    events: emitter,

    /** For convenience: Subscribe to distinct notifications here, event name = channel name */
    notifications: notificationsEmitter,

    async connect() {
      reinitializing = true
      // The first attempt is a connect; any after it are reconnects.
      let attempts = 0
      const connecting = Promise.resolve()
        .then(() => establish(() => attempts++ && emitter.emit("reconnect", attempts - 1)))
        .finally(() => {
          reinitializing = false
        })
      running = connecting
      await connecting
    },
    async close() {
      connectionLogger("Closing PostgreSQL notification listener.")
      closing = true
      wake()
      abortAttempt(new Closed())
      await running.catch(noop)
      stopWatching()
      stopWatching = noop
      const current = client
      client = null
      if (current) await current.end().catch(noop)
    },
    getSubscribedChannels() {
      return Array.from(subscribedChannels)
    },
    listenTo(channelName: string) {
      if (subscribedChannels.has(channelName)) {
        return
      }
      subscriptionLogger(`Subscribing to PostgreSQL notification "${channelName}"`)

      subscribedChannels.add(channelName)
      const current = live()
      return current ? query(current, `LISTEN ${current.escapeIdentifier(channelName)}`) : undefined
    },
    notify(channelName: string, payload?: any) {
      notificationLogger(`Sending PostgreSQL notification to "${channelName}":`, payload)

      const current = live()
      if (!current) return Promise.reject(new Error("Not connected to PostgreSQL"))
      if (payload !== undefined) {
        return query(current, "SELECT pg_notify($1, $2)", [channelName, serialize(payload)])
      } else {
        return query(current, `NOTIFY ${current.escapeIdentifier(channelName)}`)
      }
    },
    unlisten(channelName: string) {
      if (!subscribedChannels.has(channelName)) {
        return
      }
      subscriptionLogger(`Unsubscribing from PostgreSQL notification "${channelName}"`)

      subscribedChannels.delete(channelName)
      const current = live()
      return current ? query(current, `UNLISTEN ${current.escapeIdentifier(channelName)}`) : undefined
    },
    unlistenAll() {
      subscriptionLogger("Unsubscribing from all PostgreSQL notifications.")

      subscribedChannels = new Set()
      const current = live()
      return current ? query(current, "UNLISTEN *") : undefined
    }
  } as Subscriber<Events>
}

export default createPostgresSubscriber
