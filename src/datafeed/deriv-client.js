// Deriv WebSocket client — shared connection with request/response and subscription support.
// Uses the public market data gateway (no auth required).

const DERIV_WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';
const MAX_RECONNECT_DELAY = 30_000;

let socket = null;
let reconnectTimer = null;
let reconnectDelay = 1_000;
let reqIdCounter = 0;

// Pending messages queued while the socket is still connecting.
let messageQueue = [];

// Pending one-shot requests keyed by req_id for getBars / time matching.
const pendingRequests = new Map();

// Active subscriptions for realtime streaming.
// Each entry: { symbol, granularity, handlers: Map<subscriberUID, handler> }
const activeSubscriptions = new Map();
const subscriberIndex = new Map();
const subForgetId = new Map();

function hasActiveWork() {
	return activeSubscriptions.size > 0 || pendingRequests.size > 0;
}

function nextRequestId() {
	reqIdCounter += 1;
	return reqIdCounter;
}

function subKey(symbol, granularity) {
	return `${symbol}:${granularity}`;
}

// Sends a message. If the socket is still connecting, queues it instead.
function send(msg) {
	if (socket && socket.readyState === WebSocket.OPEN) {
		socket.send(JSON.stringify(msg));
		return true;
	}

	if (socket && socket.readyState === WebSocket.CONNECTING) {
		messageQueue.push(msg);
		return true;
	}

	// Socket doesn't exist yet; create it and queue.
	messageQueue.push(msg);
	createSocket();
	return true;
}

// Flushes queued messages after the socket opens or reconnects.
function flushQueue() {
	if (!socket || socket.readyState !== WebSocket.OPEN) return;

	const queue = messageQueue;
	messageQueue = [];
	queue.forEach(msg => {
		try {
			socket.send(JSON.stringify(msg));
		} catch (e) {
			console.error('[DerivClient] Error flushing queued message:', e);
		}
	});
}

function createSocket() {
	if (reconnectTimer) {
		clearTimeout(reconnectTimer);
		reconnectTimer = null;
	}

	// Close existing socket cleanly if it's in a bad state.
	if (socket && socket.readyState !== WebSocket.CLOSED) {
		try { socket.close(); } catch {}
	}

	const ws = new WebSocket(DERIV_WS_URL);

	ws.addEventListener('open', () => {
		reconnectDelay = 1_000;

		// Flush any messages queued while connecting.
		flushQueue();

		// Resubscribe all active streams after reconnect.
		activeSubscriptions.forEach(sub => {
			sendSubscription(sub.symbol, sub.granularity);
		});
	});

	ws.addEventListener('close', () => {
		if (socket === ws) socket = null;

		if (!hasActiveWork()) return;

		reconnectTimer = setTimeout(() => {
			reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY);
			createSocket();
		}, reconnectDelay);
	});

	ws.addEventListener('error', () => {
		if (ws.readyState === WebSocket.OPEN) ws.close();
	});

	ws.addEventListener('message', onMessage);
	socket = ws;
	return ws;
}

function sendSubscription(symbol, granularity) {
	send({
		ticks_history: symbol,
		adjust_start_time: 1,
		style: 'candles',
		granularity,
		subscribe: 1,
		req_id: nextRequestId(),
	});
}

function barStart(timestampMs, granularitySec) {
	const intervalMs = granularitySec * 1000;
	return Math.floor(timestampMs / intervalMs) * intervalMs;
}

function normalizeCandles(candles) {
	return candles.map(c => ({
		time: c.epoch * 1000,
		open: parseFloat(c.open),
		high: parseFloat(c.high),
		low: parseFloat(c.low),
		close: parseFloat(c.close),
	}));
}

// Routes incoming messages.
function onMessage(event) {
	let message;
	try {
		message = JSON.parse(event.data);
	} catch {
		return;
	}

	const reqId = message.req_id ?? message.echo_req?.req_id;

	// Check for errors.
	if (message.error) {
		if (reqId !== undefined && pendingRequests.has(reqId)) {
			const pending = pendingRequests.get(reqId);
			pendingRequests.delete(reqId);
			clearTimeout(pending._timer);
			pending.reject(new Error(`Deriv API error: ${message.error.message || JSON.stringify(message.error)}`));
			if (!hasActiveWork()) closeSocketIfIdle();
		}
		return;
	}

	// 1) One-shot candles response (ticks_history without subscribe).
	if (message.msg_type === 'candles' && !message.subscription) {
		if (reqId !== undefined && pendingRequests.has(reqId)) {
			const pending = pendingRequests.get(reqId);
			pendingRequests.delete(reqId);
			clearTimeout(pending._timer);
			const bars = message.candles?.length > 0
				? normalizeCandles(message.candles)
				: [];
			pending.resolve(bars);
			if (!hasActiveWork()) closeSocketIfIdle();
		}
		return;
	}

	// 2) Subscription initial data (candles batch when subscribe starts).
	if (message.msg_type === 'candles' && message.subscription) {
		const symbol = message.echo_req?.ticks_history;
		const granularity = message.echo_req?.granularity;
		if (!symbol || !granularity) return;

		const key = subKey(symbol, granularity);
		const sub = activeSubscriptions.get(key);
		if (!sub) return;

		if (message.subscription?.id) {
			subForgetId.set(key, message.subscription.id);
		}

		if (message.candles?.length > 0) {
			const bars = normalizeCandles(message.candles);
			const lastBar = bars[bars.length - 1];
			sub.handlers.forEach(handler => {
				if (!handler.initialized) {
					handler.initialized = true;
					handler.lastBar = lastBar;
				}
			});
		}
		return;
	}

	// 3) Realtime tick arriving on a subscribed stream.
	if (message.msg_type === 'tick' && message.tick) {
		const symbol = message.echo_req?.ticks_history;
		const granularity = message.echo_req?.granularity;
		if (!symbol || !granularity) return;

		const key = subKey(symbol, granularity);
		const sub = activeSubscriptions.get(key);
		if (!sub) return;

		const price = parseFloat(message.tick.quote);
		const epochMs = message.tick.epoch * 1000;

		sub.handlers.forEach(handler => {
			if (!handler.initialized || !handler.lastBar) return;

			const bar = handler.lastBar;
			const currentBarStart = barStart(epochMs, granularity);

			if (currentBarStart > bar.time) {
				handler.lastBar = {
					time: currentBarStart,
					open: price,
					high: price,
					low: price,
					close: price,
				};
			} else {
				handler.lastBar = {
					...bar,
					high: Math.max(bar.high, price),
					low: Math.min(bar.low, price),
					close: price,
				};
			}
			handler.isDirty = true;
		});
		return;
	}

	// 4) OHLC update (pre-built candle from subscription).
	if (message.msg_type === 'ohlc' && message.ohlc) {
		const symbol = message.ohlc.symbol || message.ohlc.underlying_symbol;
		const granularity = message.ohlc.granularity;
		if (!symbol || !granularity) return;

		const key = subKey(symbol, granularity);
		const sub = activeSubscriptions.get(key);
		if (!sub) return;

		const bar = {
			time: message.ohlc.epoch * 1000,
			open: parseFloat(message.ohlc.open),
			high: parseFloat(message.ohlc.high),
			low: parseFloat(message.ohlc.low),
			close: parseFloat(message.ohlc.close),
		};

		sub.handlers.forEach(handler => {
			handler.lastBar = bar;
			handler.isDirty = true;
		});
		return;
	}

	// 5) Server time response.
	if (message.msg_type === 'time') {
		if (reqId !== undefined && pendingRequests.has(reqId)) {
			const pending = pendingRequests.get(reqId);
			pendingRequests.delete(reqId);
			clearTimeout(pending._timer);
			if (typeof message.time === 'number') {
				pending.resolve(message.time);
			} else {
				pending.reject(new Error('Invalid time response'));
			}
			if (!hasActiveWork()) closeSocketIfIdle();
		}
		return;
	}
}

function closeSocketIfIdle() {
	if (socket && !hasActiveWork()) {
		try { socket.close(); } catch {}
		socket = null;
	}
}

// Batches realtime updates to reduce chart redraw pressure.
const UPDATE_FREQUENCY = 250;
setInterval(() => {
	activeSubscriptions.forEach(sub => {
		sub.handlers.forEach(handler => {
			if (!handler.isDirty || !handler.lastBar) return;
			handler.callback(handler.lastBar);
			handler.isDirty = false;
		});
	});
}, UPDATE_FREQUENCY);

// ─────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────

// Fetches historical OHLCV bars from Deriv via ticks_history.
export function fetchHistory(symbol, granularity, fromEpoch, toEpoch) {
	return new Promise((resolve, reject) => {
		const reqId = nextRequestId();
		const timer = setTimeout(() => {
			if (pendingRequests.has(reqId)) {
				pendingRequests.delete(reqId);
				reject(new Error('Deriv history request timed out'));
			}
		}, 30_000);

		pendingRequests.set(reqId, {
			_timer: timer,
			resolve: value => {
				clearTimeout(timer);
				resolve(value);
			},
			reject: err => {
				clearTimeout(timer);
				reject(err);
			},
		});

		send({
			ticks_history: symbol,
			adjust_start_time: 1,
			start: fromEpoch,
			end: toEpoch,
			style: 'candles',
			granularity,
			req_id: reqId,
		});
	});
}

// Fetches server time from Deriv.
export function fetchServerTime() {
	return new Promise((resolve, reject) => {
		const reqId = nextRequestId();
		const timer = setTimeout(() => {
			if (pendingRequests.has(reqId)) {
				pendingRequests.delete(reqId);
				reject(new Error('Deriv server time request timed out'));
			}
		}, 10_000);

		pendingRequests.set(reqId, {
			_timer: timer,
			resolve: value => {
				clearTimeout(timer);
				resolve(value);
			},
			reject: err => {
				clearTimeout(timer);
				reject(err);
			},
		});

		send({ time: 1, req_id: reqId });
	});
}

// Subscribes to realtime candle updates for a symbol at a given granularity.
export function subscribeStream(
	symbol,
	granularity,
	callback,
	subscriberUID,
	lastBar
) {
	const key = subKey(symbol, granularity);
	const handler = {
		id: subscriberUID,
		callback,
		lastBar: lastBar ?? null,
		isDirty: false,
		initialized: !!lastBar,
	};

	const existing = activeSubscriptions.get(key);
	if (existing) {
		existing.handlers.set(subscriberUID, handler);
		subscriberIndex.set(subscriberUID, key);
		return;
	}

	const handlers = new Map();
	handlers.set(subscriberUID, handler);
	activeSubscriptions.set(key, { symbol, granularity, handlers });
	subscriberIndex.set(subscriberUID, key);

	sendSubscription(symbol, granularity);
}

// Unsubscribes a subscriber from realtime updates.
export function unsubscribeStream(subscriberUID) {
	const key = subscriberIndex.get(subscriberUID);
	if (!key) return;

	subscriberIndex.delete(subscriberUID);

	const sub = activeSubscriptions.get(key);
	if (!sub) return;

	sub.handlers.delete(subscriberUID);

	if (sub.handlers.size === 0) {
		const forgetId = subForgetId.get(key);
		if (forgetId && socket && socket.readyState === WebSocket.OPEN) {
			socket.send(JSON.stringify({
				forget: forgetId,
				req_id: nextRequestId(),
			}));
		}
		subForgetId.delete(key);
		activeSubscriptions.delete(key);
		// Don't close socket immediately — let idle detection handle it.
	}
}
