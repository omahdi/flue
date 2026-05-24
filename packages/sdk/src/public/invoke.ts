import type { HttpClient } from '../http.ts';
import type { FlueEvent } from '../types.ts';
import { readSse } from './stream.ts';

export type InvokeOptions =
	| { mode: 'sync'; payload?: unknown; signal?: AbortSignal }
	| { mode: 'stream'; payload?: unknown; signal?: AbortSignal };

export type SyncInvokeResult = { result: unknown };

export function invokeAgent(
	http: HttpClient,
	name: string,
	id: string,
	options: { mode: 'stream'; payload?: unknown; signal?: AbortSignal },
): AsyncIterable<FlueEvent>;
export function invokeAgent(
	http: HttpClient,
	name: string,
	id: string,
	options: { mode: 'sync'; payload?: unknown; signal?: AbortSignal },
): Promise<SyncInvokeResult>;
export function invokeAgent(
	http: HttpClient,
	name: string,
	id: string,
	options: InvokeOptions,
): Promise<SyncInvokeResult> | AsyncIterable<FlueEvent> {
	const path = `/agents/${encodeURIComponent(name)}/${encodeURIComponent(id)}`;
	if (options.mode === 'stream') return invokeStream(http, path, options);
	return http
		.json<{ result?: unknown }>({
			method: 'POST',
			path,
			body: options.payload ?? {},
			signal: options.signal,
		})
		.then((body) => ({ result: body.result }));
}

async function* invokeStream(
	http: HttpClient,
	path: string,
	options: { payload?: unknown; signal?: AbortSignal },
): AsyncIterable<FlueEvent> {
	const response = await http.fetchImpl(http.url(path), {
		method: 'POST',
		headers: await http.requestHeaders({ accept: 'text/event-stream' }, true),
		body: JSON.stringify(options.payload ?? {}),
		signal: options.signal,
	});
	if (!response.ok) throw new Error(`Invocation stream failed with HTTP ${response.status}.`);
	if (!response.body) throw new Error('Invocation stream response has no body.');
	for await (const frame of readSse(response.body)) {
		yield JSON.parse(frame.data) as FlueEvent;
	}
}
