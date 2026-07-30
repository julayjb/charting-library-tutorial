// Deriv WebSocket client — shared connection with request/response and subscription support.
// Uses the public market data gateway (no auth required).

const DERIV_WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';
const MAX_RECONNECT_DELAY = 30_000;
const SOCKET_CONNECT_DELAY_MS = 100;

let socket = null;
let connectTimer = null;
let reconnectTimer = null;
let reconnectDelay = 1_000;
let hasConnectedBefore = false;
let reqIdCounter = 0;

// Pending one-shot requests keyed by req_id for getBars history matching.
const pendingRequests = new Map();

// Active subscriptions for realtime streaming.
// Each entry: { symbol, granularity, subId, handlers: Map<subscriberUID, handler> }
const activeSubscriptions = new Map();
// Maps subscriberUID -> subscription key for fast unsubscription.
const subscriberIndex = new Map();
// Maps subscription key -> { forgetId } for unsubscribe via forget API.
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

function send(msg) {
	if (!socket || socket.readyState !== WebSocket.OPEN) return false;
	socket.send(JSON.stringify(msg));
	return true;
}

function ensureSocket() {
	if (
		socket &&
		(socket.readyState === WebSocket.CONNECTING ||
			socket.readyState === WebSocket.OPEN)
	) {
		return socket;
	}

	if (!connectTimer) {
		connectTimer = setTimeout(() => {
			connectTimer = null;
			if (hasActiveWork()) {
				socket = createSocket();
			}
		}, SOCKET_CONNECT_DELAY_MS);
	}

	return socket;
}

function stopSocketWorkIfIdle() {
	if (hasActiveWork()) return;

	if (connectTimer) {
		clearTimeout(connectTimer);
		connectTimer = null;
	}
	if (reconnectTimer) {
		clearTimeout(reconnectTimer);
		reconnectTimer = null;
	}
	reconnectDelay = 1_000;
}

function createSocket() {
	if (connectTimer) {
		clearTimeout(connectTimer);
		connectTimer = null;
	}
	if (reconnectTimer) {
		clearTimeout(reconnectTimer);
		reconnectTimer = null;
	}

	const ws = new WebSocket(DERIV_WS_URL);

	ws.addEventListener('open', () => {
		hasConnectedBefore = true;
		reconnectDelay = 1_000;

		// Resubscribe all active streams after reconnect.
		activeSubscriptions.forEach(sub => {
			sendSubscription(ws, sub.symbol, sub.granularity);
		});
	});

	ws.addEventListener('close', () => {
		if (socket === ws) socket = null;
		if (!hasActiveWork()) return;

		reconnectTimer = setTimeout(() => {
			reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY);
			socket = createSocket();
		}, reconnectDelay);
	});

	ws.addEventListener('error', () => {
		if (ws.readyState === WebSocket.OPEN) ws.close();
	});

	ws.addEventListener('message', onMessage);
	return ws;
}

function sendSubscription(ws, symbol, granularity) {
	if (!ws || ws.readyState !== WebSocket.OPEN) return;

	ws.send(
		JSON.stringify({
			ticks_history: symbol,
			adjust_start_time: 1,
			style: 'candles',
			granularity,
			subscribe: 1,
			req_id: nextRequestId(),
		})
	);
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

	// Check for errors first.
	if (message.error) {
		const reqId = message.echo_req?.req_id;
		if (reqId !== undefined && pendingRequests.has(reqId)) {
			const pending = pendingRequests.get(reqId);
			pendingRequests.delete(reqId);
			pending.reject(new Error(`Deriv API error: ${message.error.message}`));
			if (!hasActiveWork()) closeSocketIfIdle();
		}
		return;
	}

	const reqId = message.req_id ?? message.echo_req?.req_id;

	// 1) One-shot request response (ticks_history without subscribe).
	if (message.msg_type === 'candles' && !message.subscription) {
		if (reqId !== undefined && pendingRequests.has(reqId)) {
			const pending = pendingRequests.get(reqId);
			pendingRequests.delete(reqId);
			const bars = message.candles?.length > 0
				? normalizeCandles(message.candles)
				: [];
			pending.resolve(bars);
			if (!hasActiveWork()) closeSocketIfIdle();
		}
		return;
	}

	// 2) Subscription initial data (first candles batch when subscribe starts).
	if (message.msg_type === 'candles' && message.subscription) {
		const symbol = message.echo_req?.ticks_history;
		const granularity = message.echo_req?.granularity;
		if (!symbol || !granularity) return;

		const key = subKey(symbol, granularity);
		const sub = activeSubscriptions.get(key);
		if (!sub) return;

		// Remember subscription id for forget() on unsubscribe.
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

	// 3) Realtime tick arriving on a subscribed ticks_history stream.
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

	// 5) Server time response — resolve pending request.
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
		socket.close();
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
			resolve: value => {
				clearTimeout(timer);
				resolve(value);
			},
			reject: err => {
				clearTimeout(timer);
				reject(err);
			},
		});

		const msg = {
			ticks_history: symbol,
			adjust_start_time: 1,
			start: fromEpoch,
			end: toEpoch,
			style: 'candles',
			granularity,
			req_id: reqId,
		};

		ensureSocket();

		if (!send(msg)) {
			const retryOnOpen = () => {
				if (socket && socket.readyState === WebSocket.OPEN) {
					socket.removeEventListener('open', retryOnOpen);
					send(msg);
				}
			};
			if (socket) {
				socket.addEventListener('open', retryOnOpen);
			} else {
				setTimeout(() => {
					if (socket && socket.readyState === WebSocket.OPEN) {
						send(msg);
					}
				}, SOCKET_CONNECT_DELAY_MS + 100);
			}
		}
	});
}

// Fetches server time from Deriv (for bar countdown synchronization).
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
			_resolve: resolve,
			_reject: reject,
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

		const msg = {
			time: 1,
			req_id: reqId,
		};

		ensureSocket();

		if (!send(msg)) {
			const retryOnOpen = () => {
				if (socket && socket.readyState === WebSocket.OPEN) {
					socket.removeEventListener('open', retryOnOpen);
					send(msg);
				}
			};
			if (socket) {
				socket.addEventListener('open', retryOnOpen);
			} else {
				setTimeout(() => {
					if (socket && socket.readyState === WebSocket.OPEN) {
						send(msg);
					}
				}, SOCKET_CONNECT_DELAY_MS + 100);
			}
		}
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
	activeSubscriptions.set(key, {
		symbol,
		granularity,
		handlers,
	});
	subscriberIndex.set(subscriberUID, key);

	sendSubscription(ensureSocket(), symbol, granularity);
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
		// Send forget to cancel the subscription on the server.
		const forgetId = subForgetId.get(key);
		if (forgetId && socket && socket.readyState === WebSocket.OPEN) {
			socket.send(JSON.stringify({
				forget: forgetId,
				req_id: nextRequestId(),
			}));
		}
		subForgetId.delete(key);
		activeSubscriptions.delete(key);
		stopSocketWorkIfIdle();
	}
}
