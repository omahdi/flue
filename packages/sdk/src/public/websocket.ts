import type {
	AgentWebSocketClientMessage,
	FlueEvent,
	FluePublicError,
	WebSocketServerMessage,
	WorkflowWebSocketClientMessage,
} from '../types.ts';

export interface WebSocketLike {
	addEventListener(type: 'message' | 'close' | 'error', listener: (event: unknown) => void): void;
	send(data: string): void;
	close(code?: number, reason?: string): void;
}

export type WebSocketFactory = (url: string) => WebSocketLike;

export interface SocketInvokeResult {
	result: unknown;
	runId?: string;
}

export interface SocketEventContext {
	requestId: string;
	runId?: string;
}

export type SocketEventListener = (event: FlueEvent, context: SocketEventContext) => void;

export interface AgentSocket {
	readonly ready: Promise<void>;
	prompt(message: string, options?: { session?: string }): Promise<SocketInvokeResult>;
	ping(): Promise<void>;
	onEvent(listener: SocketEventListener): () => void;
	close(code?: number, reason?: string): void;
}

export interface WorkflowSocket {
	readonly ready: Promise<void>;
	invoke(payload?: unknown): Promise<SocketInvokeResult>;
	onEvent(listener: SocketEventListener): () => void;
	close(code?: number, reason?: string): void;
}

export class FlueSocketError extends Error {
	readonly error: FluePublicError;
	readonly requestId: string | undefined;
	readonly runId: string | undefined;

	constructor(error: FluePublicError, context: { requestId?: string; runId?: string } = {}) {
		super(error.message);
		this.name = 'FlueSocketError';
		this.error = error;
		this.requestId = context.requestId;
		this.runId = context.runId;
	}
}

type PendingRequest = {
	resolve(value: SocketInvokeResult): void;
	reject(error: Error): void;
};

type PendingPing = {
	resolve(): void;
	reject(error: Error): void;
};

class ProtocolSocket {
	readonly ready: Promise<void>;
	private readonly socket: WebSocketLike;
	private readonly acceptsReady: (message: WebSocketServerMessage) => boolean;
	private readonly pendingRequests = new Map<string, PendingRequest>();
	private readonly pendingPings = new Map<string, PendingPing>();
	private readonly listeners = new Set<SocketEventListener>();
	private resolveReady!: () => void;
	private rejectReady!: (error: Error) => void;
	private isReady = false;
	private isClosed = false;
	private terminalError: Error | undefined;
	private sequence = 0;

	constructor(socket: WebSocketLike, acceptsReady: (message: WebSocketServerMessage) => boolean) {
		this.socket = socket;
		this.acceptsReady = acceptsReady;
		this.ready = new Promise<void>((resolve, reject) => {
			this.resolveReady = resolve;
			this.rejectReady = reject;
		});
		this.socket.addEventListener('message', (event) => this.receive(event));
		this.socket.addEventListener('close', () => this.fail(new Error('Flue WebSocket connection closed.')));
		this.socket.addEventListener('error', () => this.fail(new Error('Flue WebSocket connection failed.')));
	}

	onEvent(listener: SocketEventListener): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	close(code?: number, reason?: string): void {
		this.fail(new Error('Flue WebSocket connection closed.'));
		this.socket.close(code, reason);
	}

	async request(message: Extract<AgentWebSocketClientMessage, { type: 'prompt' }> | WorkflowWebSocketClientMessage): Promise<SocketInvokeResult> {
		await this.ready;
		this.assertOpen();
		return new Promise<SocketInvokeResult>((resolve, reject) => {
			this.pendingRequests.set(message.requestId, { resolve, reject });
			try {
				this.socket.send(JSON.stringify(message));
			} catch (error) {
				this.pendingRequests.delete(message.requestId);
				reject(asError(error));
			}
		});
	}

	async ping(): Promise<void> {
		await this.ready;
		this.assertOpen();
		const requestId = this.requestId();
		return new Promise<void>((resolve, reject) => {
			this.pendingPings.set(requestId, { resolve, reject });
			try {
				this.socket.send(JSON.stringify({ version: 1, type: 'ping', requestId } satisfies AgentWebSocketClientMessage));
			} catch (error) {
				this.pendingPings.delete(requestId);
				reject(asError(error));
			}
		});
	}

	requestId(): string {
		this.sequence += 1;
		const random = globalThis.crypto?.randomUUID?.() ?? Math.random().toString(36).slice(2);
		return `req_${random}_${this.sequence}`;
	}

	private receive(event: unknown): void {
		const raw = messageData(event);
		const message = raw === undefined ? undefined : parseServerMessage(raw);
		if (!message) {
			this.protocolFailure();
			return;
		}
		if (message.type === 'ready') {
			if (!this.isReady && this.acceptsReady(message)) {
				this.isReady = true;
				this.resolveReady();
				return;
			}
			this.protocolFailure();
			return;
		}
		if (!this.isReady) {
			this.protocolFailure();
			return;
		}
		switch (message.type) {
			case 'started':
				return;
			case 'event':
				for (const listener of this.listeners) listener(message.event, { requestId: message.requestId, ...(message.runId === undefined ? {} : { runId: message.runId }) });
				return;
			case 'result': {
				const pending = this.pendingRequests.get(message.requestId);
				if (!pending) return;
				this.pendingRequests.delete(message.requestId);
				pending.resolve({ result: message.result, ...(message.runId === undefined ? {} : { runId: message.runId }) });
				return;
			}
			case 'error': {
				const error = new FlueSocketError(message.error, { requestId: message.requestId, runId: message.runId });
				if (message.requestId) {
					const pending = this.pendingRequests.get(message.requestId);
					if (pending) {
						this.pendingRequests.delete(message.requestId);
						pending.reject(error);
						return;
					}
					const ping = this.pendingPings.get(message.requestId);
					if (ping) {
						this.pendingPings.delete(message.requestId);
						ping.reject(error);
						return;
					}
				}
				this.fail(error);
				this.socket.close(1011, 'WebSocket error');
				return;
			}
			case 'pong': {
				if (!message.requestId) return;
				const pending = this.pendingPings.get(message.requestId);
				if (!pending) return;
				this.pendingPings.delete(message.requestId);
				pending.resolve();
				return;
			}
		}
	}

	private protocolFailure(): void {
		this.fail(new Error('Flue WebSocket received an invalid protocol message.'));
		this.socket.close(1008, 'Invalid protocol message');
	}

	private assertOpen(): void {
		if (this.isClosed) throw this.terminalError ?? new Error('Flue WebSocket connection is closed.');
	}

	private fail(error: Error): void {
		if (this.isClosed) return;
		this.isClosed = true;
		this.terminalError = error;
		if (!this.isReady) this.rejectReady(error);
		for (const pending of this.pendingRequests.values()) pending.reject(error);
		for (const pending of this.pendingPings.values()) pending.reject(error);
		this.pendingRequests.clear();
		this.pendingPings.clear();
		this.listeners.clear();
	}
}

class AgentSocketClient implements AgentSocket {
	readonly ready: Promise<void>;
	private readonly connection: ProtocolSocket;

	constructor(socket: WebSocketLike, name: string, id: string) {
		this.connection = new ProtocolSocket(
			socket,
			(message) => message.type === 'ready' && message.target === 'agent' && message.name === name && message.instanceId === id,
		);
		this.ready = this.connection.ready;
	}

	prompt(message: string, options: { session?: string } = {}): Promise<SocketInvokeResult> {
		return this.connection.request({
			version: 1,
			type: 'prompt',
			requestId: this.connection.requestId(),
			message,
			...(options.session === undefined ? {} : { session: options.session }),
		});
	}

	ping(): Promise<void> {
		return this.connection.ping();
	}

	onEvent(listener: SocketEventListener): () => void {
		return this.connection.onEvent(listener);
	}

	close(code?: number, reason?: string): void {
		this.connection.close(code, reason);
	}
}

class WorkflowSocketClient implements WorkflowSocket {
	readonly ready: Promise<void>;
	private readonly connection: ProtocolSocket;
	private invoked = false;

	constructor(socket: WebSocketLike, name: string) {
		this.connection = new ProtocolSocket(socket, (message) => message.type === 'ready' && message.target === 'workflow' && message.name === name);
		this.ready = this.connection.ready;
	}

	invoke(payload?: unknown): Promise<SocketInvokeResult> {
		if (this.invoked) return Promise.reject(new Error('A workflow WebSocket accepts only one invocation.'));
		this.invoked = true;
		return this.connection.request({
			version: 1,
			type: 'invoke',
			requestId: this.connection.requestId(),
			...(payload === undefined ? {} : { payload }),
		});
	}

	onEvent(listener: SocketEventListener): () => void {
		return this.connection.onEvent(listener);
	}

	close(code?: number, reason?: string): void {
		this.connection.close(code, reason);
	}
}

export function connectAgentSocket(factory: WebSocketFactory, url: string, name: string, id: string): AgentSocket {
	return new AgentSocketClient(factory(url), name, id);
}

export function connectWorkflowSocket(factory: WebSocketFactory, url: string, name: string): WorkflowSocket {
	return new WorkflowSocketClient(factory(url), name);
}

export function webSocketUrl(httpUrl: string): string {
	const url = new URL(httpUrl);
	if (url.protocol === 'https:') url.protocol = 'wss:';
	else if (url.protocol === 'http:') url.protocol = 'ws:';
	else throw new Error(`Flue WebSocket requires an HTTP base URL, received ${url.protocol}`);
	return url.toString();
}

export function defaultWebSocketFactory(url: string): WebSocketLike {
	const Socket = (globalThis as { WebSocket?: new (url: string) => WebSocketLike }).WebSocket;
	if (!Socket) throw new Error('WebSocket is not available in this environment. Configure a websocket factory.');
	return new Socket(url);
}

function parseServerMessage(value: string): WebSocketServerMessage | undefined {
	let parsed: unknown;
	try {
		parsed = JSON.parse(value);
	} catch {
		return undefined;
	}
	if (!isRecord(parsed) || parsed.version !== 1 || typeof parsed.type !== 'string') return undefined;
	switch (parsed.type) {
		case 'ready':
			if (parsed.target === 'agent' && typeof parsed.name === 'string' && typeof parsed.instanceId === 'string') return parsed as unknown as WebSocketServerMessage;
			if (parsed.target === 'workflow' && typeof parsed.name === 'string') return parsed as unknown as WebSocketServerMessage;
			return undefined;
		case 'started':
		case 'result':
			if (typeof parsed.requestId === 'string' && (parsed.runId === undefined || typeof parsed.runId === 'string')) return parsed as unknown as WebSocketServerMessage;
			return undefined;
		case 'event':
			if (
				typeof parsed.requestId === 'string' &&
				(parsed.runId === undefined || typeof parsed.runId === 'string') &&
				isRecord(parsed.event) &&
				typeof parsed.event.type === 'string'
			) {
				return parsed as unknown as WebSocketServerMessage;
			}
			return undefined;
		case 'error':
			if (!isPublicError(parsed.error)) return undefined;
			if (parsed.requestId !== undefined && typeof parsed.requestId !== 'string') return undefined;
			if (parsed.runId !== undefined && typeof parsed.runId !== 'string') return undefined;
			return parsed as unknown as WebSocketServerMessage;
		case 'pong':
			if (parsed.requestId === undefined || typeof parsed.requestId === 'string') return parsed as unknown as WebSocketServerMessage;
			return undefined;
		default:
			return undefined;
	}
}

function isPublicError(value: unknown): value is FluePublicError {
	return isRecord(value) && typeof value.type === 'string' && typeof value.message === 'string' && typeof value.details === 'string';
}

function messageData(event: unknown): string | undefined {
	if (!isRecord(event)) return undefined;
	return typeof event.data === 'string' ? event.data : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === 'object' && value !== null;
}

function asError(error: unknown): Error {
	return error instanceof Error ? error : new Error(String(error));
}
