"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const debug_1 = __importDefault(require("debug"));
const events_1 = require("events");
const pg_1 = __importDefault(require("pg"));
const connectionLogger = (0, debug_1.default)("pg-listen:connection");
const notificationLogger = (0, debug_1.default)("pg-listen:notification");
const paranoidLogger = (0, debug_1.default)("pg-listen:paranoid");
const subscriptionLogger = (0, debug_1.default)("pg-listen:subscription");
/** Thrown inside the connection loop when close() was called: stop quietly. */
class Closed extends Error {
    constructor() {
        super("The subscriber was closed");
    }
}
/** Thrown when retryLimit or retryTimeout ran out: give up and say so. */
class GaveUp extends Error {
}
const noop = () => undefined;
function withTimeout(promise, ms, what) {
    let timer;
    const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}
/** Queries on a client, one at a time: node-postgres 9 drops its own queue
 *  for overlapping queries on one client. */
const queues = new WeakMap();
function query(client, text, values) {
    const run = (queues.get(client) ?? Promise.resolve()).then(() => client.query(text, values));
    queues.set(client, run.catch(noop));
    return run;
}
/** Hang up on a client we're done with. end() says goodbye and waits for
 *  the server's, which a stalled or half-open connection never sends.
 *  node-postgres destroys the socket itself only if a query is running, so
 *  it's destroyed here shortly after (there's no public way to do that). */
function retire(client) {
    client.end().catch(noop);
    const stream = client.connection?.stream;
    setTimeout(() => stream?.destroy?.(), 1000).unref();
}
function createPostgresSubscriber(connectionConfig, options = {}) {
    const { paranoidChecking = 30000, parse = JSON.parse, serialize = JSON.stringify, retryInterval = (attempt) => Math.min(500 * 2 ** attempt, 10000), retryLimit = Infinity, retryTimeout = 0 } = options;
    const paranoidTimeout = options.paranoidTimeout ?? Math.min(paranoidChecking || 10000, 10000);
    const getRetryInterval = typeof retryInterval === "function" ? retryInterval : () => retryInterval;
    // A connect that never hears back (a host that accepts and goes silent)
    // would otherwise stall reconnecting forever.
    const config = { connectionTimeoutMillis: 10000, ...connectionConfig, keepAlive: true };
    const Client = options.native && pg_1.default.native ? pg_1.default.native.Client : pg_1.default.Client;
    const emitter = new events_1.EventEmitter();
    emitter.setMaxListeners(0); // unlimited listeners
    const notificationsEmitter = new events_1.EventEmitter();
    notificationsEmitter.setMaxListeners(0); // unlimited listeners
    emitter.on("notification", (notification) => {
        notificationsEmitter.emit(notification.channel, notification.payload);
    });
    let client = null;
    let closing = false;
    let reinitializing = false;
    let subscribedChannels = new Set();
    /** Clients whose connection was lost, possibly while we were still setting them up. */
    const lost = new WeakSet();
    let stopWatching = noop;
    // A retry wait that close() can cut short.
    let wake = noop;
    const sleep = (ms) => new Promise(resolve => {
        const timer = setTimeout(resolve, ms);
        wake = () => {
            clearTimeout(timer);
            resolve();
        };
    });
    /** Rejects the connection attempt in flight, for close(). */
    let abortAttempt = noop;
    /** Open a connection, retrying per the options. */
    const open = async (onAttempt) => {
        const startTime = Date.now();
        for (let attempt = 1;; attempt++) {
            if (closing)
                throw new Closed();
            onAttempt(attempt);
            // A listener may have closed us.
            if (closing)
                throw new Closed();
            connectionLogger(`PostgreSQL connection attempt #${attempt}...`);
            const next = new Client(config);
            // Permanent: a client can error more than once (or after we've given up
            // on it), and an "error" with no listener would crash the process.
            next.on("error", (error) => connectionLogger("DB client error:", error));
            try {
                // Ending a client mid-connect never settles its connect() (node-postgres
                // drops the callback once it's ending), so close() rejects this instead.
                const aborted = new Promise((_, reject) => (abortAttempt = reject));
                await Promise.race([next.connect(), aborted]);
            }
            catch (error) {
                retire(next);
                if (closing)
                    throw new Closed();
                connectionLogger(`PostgreSQL connection attempt #${attempt} failed:`, error);
                const message = error instanceof Error ? error.message : String(error);
                if (attempt >= retryLimit) {
                    throw new GaveUp(`Connecting to PostgreSQL failed after ${attempt} attempts: ${message}`);
                }
                if (retryTimeout && Date.now() - startTime > retryTimeout) {
                    throw new GaveUp(`Stopping PostgreSQL reconnection attempts after ${retryTimeout}ms timeout has been reached: ${message}`);
                }
                await sleep(getRetryInterval(attempt - 1));
                continue;
            }
            finally {
                abortAttempt = noop;
            }
            if (closing) {
                retire(next);
                throw new Closed();
            }
            connectionLogger("PostgreSQL connection succeeded");
            return next;
        }
    };
    /** Wire up a fresh client: forward its notifications, notice when it's
     *  lost, and LISTEN on every subscribed channel (including ones added
     *  while this ran). */
    const start = async (next) => {
        client = next;
        const onNotification = (notification) => {
            // Not subscribed (any more): e.g. unlistened while we were reconnecting.
            if (!subscribedChannels.has(notification.channel))
                return;
            notificationLogger(`Received PostgreSQL notification on "${notification.channel}":`, notification.payload);
            let payload;
            try {
                payload = notification.payload ? parse(notification.payload) : undefined;
            }
            catch (error) {
                error.message = `Error parsing PostgreSQL notification payload: ${error.message}`;
                emitter.emit("error", error);
                return;
            }
            emitter.emit("notification", {
                processId: notification.processId,
                channel: notification.channel,
                payload
            });
        };
        const onLost = (why) => () => {
            if (lost.has(next))
                return;
            lost.add(next);
            connectionLogger(`DB client ${why}`);
            if (client === next)
                void reinitialize();
        };
        const onError = onLost("errored");
        const onEnd = onLost("connection ended");
        next.on("notification", onNotification);
        next.on("error", onError);
        next.on("end", onEnd);
        let interval;
        if (paranoidChecking) {
            let checking = false;
            interval = setInterval(async () => {
                if (checking)
                    return;
                checking = true;
                try {
                    await withTimeout(query(next, "SELECT 1"), paranoidTimeout, "Paranoid connection check");
                    paranoidLogger("Paranoid connection check ok");
                }
                catch (error) {
                    paranoidLogger("Paranoid connection check failed:", error);
                    onLost("failed the paranoid check")();
                }
                finally {
                    checking = false;
                }
            }, paranoidChecking);
        }
        stopWatching = () => {
            clearInterval(interval);
            next.removeListener("notification", onNotification);
            next.removeListener("error", onError);
            next.removeListener("end", onEnd);
        };
        const listening = new Set();
        for (;;) {
            const missing = Array.from(subscribedChannels).filter(channel => !listening.has(channel));
            if (missing.length === 0)
                break;
            subscriptionLogger(`Subscribing to channels: ${missing.join(", ")}`);
            for (const channel of missing)
                await query(next, `LISTEN ${next.escapeIdentifier(channel)}`);
            missing.forEach(channel => listening.add(channel));
        }
        if (lost.has(next))
            throw new Error("Connection lost while subscribing");
    };
    /** Connect and listen, retrying until it works, close() is called or the
     *  retry options give up. */
    const establish = async (onAttempt) => {
        for (;;) {
            const next = await open(onAttempt);
            try {
                await start(next);
                if (closing)
                    throw new Closed();
                emitter.emit("connected");
                return;
            }
            catch (error) {
                connectionLogger("Setting up the connection failed, reconnecting:", error);
                stopWatching();
                stopWatching = noop;
                retire(next);
                if (closing)
                    throw new Closed();
            }
        }
    };
    /** The connect or reconnect in progress, for close() to wait out. */
    let running = Promise.resolve();
    // Handles its own errors: emits "error" if it gives up.
    const reinitialize = () => {
        if (reinitializing || closing)
            return;
        reinitializing = true;
        // Started a tick later, so `running` is this loop before any of its
        // events fire: a close() from a "reconnect" listener waits for it.
        running = Promise.resolve().then(recover);
    };
    const recover = async () => {
        try {
            stopWatching();
            stopWatching = noop;
            if (client)
                retire(client);
            client = null;
            // Every attempt here is a reconnect, the first one included.
            let attempts = 0;
            await establish(() => emitter.emit("reconnect", ++attempts));
        }
        catch (error) {
            if (error instanceof Closed)
                return;
            error.message = `Re-initializing the PostgreSQL notification client after connection loss failed: ${error.message}`;
            connectionLogger(error.stack || error);
            emitter.emit("error", error);
        }
        finally {
            reinitializing = false;
        }
    };
    /** The client to send a query on now, or null while (re)connecting. */
    const live = () => (reinitializing ? null : client);
    return {
        /** Emits events: "connected", "error", "notification" & "reconnect" */
        events: emitter,
        /** For convenience: Subscribe to distinct notifications here, event name = channel name */
        notifications: notificationsEmitter,
        async connect() {
            reinitializing = true;
            // The first attempt is a connect; any after it are reconnects.
            let attempts = 0;
            const connecting = Promise.resolve()
                .then(() => establish(() => attempts++ && emitter.emit("reconnect", attempts - 1)))
                .finally(() => {
                reinitializing = false;
            });
            running = connecting;
            await connecting;
        },
        async close() {
            connectionLogger("Closing PostgreSQL notification listener.");
            closing = true;
            wake();
            abortAttempt(new Closed());
            await running.catch(noop);
            stopWatching();
            stopWatching = noop;
            const current = client;
            client = null;
            if (current)
                await current.end().catch(noop);
        },
        getSubscribedChannels() {
            return Array.from(subscribedChannels);
        },
        listenTo(channelName) {
            if (subscribedChannels.has(channelName)) {
                return;
            }
            subscriptionLogger(`Subscribing to PostgreSQL notification "${channelName}"`);
            subscribedChannels.add(channelName);
            const current = live();
            return current ? query(current, `LISTEN ${current.escapeIdentifier(channelName)}`) : undefined;
        },
        notify(channelName, payload) {
            notificationLogger(`Sending PostgreSQL notification to "${channelName}":`, payload);
            const current = live();
            if (!current)
                return Promise.reject(new Error("Not connected to PostgreSQL"));
            if (payload !== undefined) {
                return query(current, "SELECT pg_notify($1, $2)", [channelName, serialize(payload)]);
            }
            else {
                return query(current, `NOTIFY ${current.escapeIdentifier(channelName)}`);
            }
        },
        unlisten(channelName) {
            if (!subscribedChannels.has(channelName)) {
                return;
            }
            subscriptionLogger(`Unsubscribing from PostgreSQL notification "${channelName}"`);
            subscribedChannels.delete(channelName);
            const current = live();
            return current ? query(current, `UNLISTEN ${current.escapeIdentifier(channelName)}`) : undefined;
        },
        unlistenAll() {
            subscriptionLogger("Unsubscribing from all PostgreSQL notifications.");
            subscribedChannels = new Set();
            const current = live();
            return current ? query(current, "UNLISTEN *") : undefined;
        }
    };
}
exports.default = createPostgresSubscriber;
