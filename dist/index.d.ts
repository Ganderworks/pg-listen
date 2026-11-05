import pg from "pg";
export interface PgParsedNotification {
    processId: number;
    channel: string;
    payload?: any;
}
interface PgListenEvents {
    /** Connected and listening on every subscribed channel. After a reconnect,
     *  this is the moment to catch up on whatever was sent while disconnected:
     *  Postgres doesn't keep notifications for a listener that wasn't there. */
    connected: () => void;
    /** A payload that didn't parse, or reconnecting gave up (retryLimit,
     *  retryTimeout). Like any EventEmitter, an "error" nobody listens to throws. */
    error: (error: Error) => void;
    notification: (notification: PgParsedNotification) => void;
    /** A connection attempt after the first: `attempt` counts from 1. */
    reconnect: (attempt: number) => void;
}
type Arguments<T> = [T] extends [(...args: infer U) => any] ? U : [T] extends [void] ? [] : [T];
/** An EventEmitter whose event names and listener arguments are typed. */
export interface TypedEventEmitter<Events> {
    addListener<E extends keyof Events>(event: E, listener: Events[E]): this;
    on<E extends keyof Events>(event: E, listener: Events[E]): this;
    once<E extends keyof Events>(event: E, listener: Events[E]): this;
    prependListener<E extends keyof Events>(event: E, listener: Events[E]): this;
    prependOnceListener<E extends keyof Events>(event: E, listener: Events[E]): this;
    off<E extends keyof Events>(event: E, listener: Events[E]): this;
    removeAllListeners<E extends keyof Events>(event?: E): this;
    removeListener<E extends keyof Events>(event: E, listener: Events[E]): this;
    emit<E extends keyof Events>(event: E, ...args: Arguments<Events[E]>): boolean;
    eventNames(): (keyof Events)[];
    listeners<E extends keyof Events>(event: E): Function[];
    listenerCount<E extends keyof Events>(event: E): number;
    getMaxListeners(): number;
    setMaxListeners(maxListeners: number): this;
}
type EventsToEmitterHandlers<Events extends Record<string, any>> = {
    [channelName in keyof Events]: (payload: Events[channelName]) => void;
};
export interface Options {
    /**
     * Using native PG client? Defaults to false.
     */
    native?: boolean;
    /**
     * Interval in ms to run a trivial query on the DB to see if
     * the database connection still works.
     * Defaults to 30s.
     */
    paranoidChecking?: number | false;
    /**
     * How long the paranoid check's query may take before the connection
     * counts as dead. A half-open TCP connection (the server or a NAT box
     * dropped it without telling us) never errors on its own; this is what
     * catches it. Defaults to the paranoid interval, at most 10s.
     */
    paranoidTimeout?: number;
    /**
     * How much time to wait between connection attempts (if failed).
     * Can also be a callback taking the attempt (from 0) and returning a
     * delay in milliseconds. Defaults to 500ms, doubling up to 10s.
     */
    retryInterval?: number | ((attempt: number) => number);
    /**
     * How many connection attempts before giving up. Defaults to no limit.
     */
    retryLimit?: number;
    /**
     * Time in ms after which to stop retrying and just fail. Defaults to 0:
     * never stop. (Upstream stopped after 3s, leaving the subscriber dead for
     * good after any outage longer than that.)
     */
    retryTimeout?: number;
    /**
     * Custom function to control how the payload data is parsed.
     * Use together with the `serialize` option. Defaults to `JSON.parse`.
     */
    parse?: (serialized: string) => any;
    /**
     * Custom function to control how the payload data is stringified on `.notify()`.
     * Use together with the `parse` option. Defaults to `JSON.stringify`.
     */
    serialize?: (data: any) => string;
}
export interface Subscriber<Events extends Record<string, any> = {
    [channel: string]: any;
}> {
    /** Emits events: "connected", "error", "notification" & "reconnect" */
    events: TypedEventEmitter<PgListenEvents>;
    /** For convenience: Subscribe to distinct notifications here, event name = channel name */
    notifications: TypedEventEmitter<EventsToEmitterHandlers<Events>>;
    /** Connect, retrying (per the retry options) until it does. Resolves once
     *  listening on every channel subscribed so far. */
    connect(): Promise<void>;
    /** Hang up and stop reconnecting. Resolves once nothing is left running. */
    close(): Promise<void>;
    getSubscribedChannels(): string[];
    /** Subscribe. While disconnected, the channel is listened to on reconnect. */
    listenTo(channelName: string): Promise<pg.QueryResult> | undefined;
    notify<EventName extends keyof Events>(channelName: any extends Events[EventName] ? EventName : void extends Events[EventName] ? never : EventName, payload: Events[EventName] extends void ? never : Events[EventName]): Promise<pg.QueryResult>;
    notify<EventName extends keyof Events>(channelName: void extends Events[EventName] ? EventName : never): Promise<pg.QueryResult>;
    unlisten(channelName: string): Promise<pg.QueryResult> | undefined;
    unlistenAll(): Promise<pg.QueryResult> | undefined;
}
declare function createPostgresSubscriber<Events extends Record<string, any> = {
    [channel: string]: any;
}>(connectionConfig?: pg.ClientConfig, options?: Options): Subscriber<Events>;
export default createPostgresSubscriber;
