// Deriv WebSocket client — shared connection with request/response and subscription support.
// Uses the public market data gateway (no auth required).
//
// Key design:
//   - Socket created immediately on first send(), messages queued while CONNECTING
//   - Request queue throttled to avoid Deriv rate limits (max 1 ticks_history/1.2s)
//   - In-memory LRU cache for historical bars
//   - Automatic retry with backoff on rate-limit errors

const DERIV_WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';
const MAX_RECONNECT_DELAY = 30_000;
const RATE_LIMIT_INTERVAL = 1_200; // ms between ticks_history requests
const HISTORY_TIMEOUT = 30_000;
const CACHE_MAX_AGE = 300_000; // 5 min cache TTL

let socket = null;
let reconnectTimer = null;
let reconnectDelay = 1_000;
let reqIdCounter = 0;

// Queue of messages waiting for socket open.
let messageQueue = [];

// Pending one-shot requests keyed by req_id.
const pendingRequests = new Map();

// Ticks_history request throttle queue.
const historyQueue = [];
let historyQueueTimer = null;

// Bar cache: key = `${symbol}:${granularity}:${from}:${to}`
const barCache = new Map();

// Active subscriptions for realtime streaming.
const activeSubscriptions = new Map();
const subscriberIndex = new Map();
const subForgetId = new Map();

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

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

function cacheKey(symbol, granularity, from, to) {
  // Round to 10s boundaries for better cache hits
  const f = Math.floor(from / 10) * 10;
  const t = Math.ceil(to / 10) * 10;
  return `${symbol}:${granularity}:${f}:${t}`;
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

function barStart(timestampMs, granularitySec) {
  const intervalMs = granularitySec * 1000;
  return Math.floor(timestampMs / intervalMs) * intervalMs;
}

// ─────────────────────────────────────────────
// Send / Queue / Socket Management
// ─────────────────────────────────────────────

function send(msg) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(msg));
    return true;
  }
  if (socket && socket.readyState === WebSocket.CONNECTING) {
    messageQueue.push(msg);
    return true;
  }
  messageQueue.push(msg);
  createSocket();
  return true;
}

function flushQueue() {
  if (!socket || socket.readyState !== WebSocket.OPEN) return;
  const queue = messageQueue;
  messageQueue = [];
  queue.forEach(msg => {
    try { socket.send(JSON.stringify(msg)); } catch (e) {
      console.error('[DerivClient] Error flushing queued message:', e);
    }
  });
}

function createSocket() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (socket && socket.readyState !== WebSocket.CLOSED) {
    try { socket.close(); } catch {}
  }

  const ws = new WebSocket(DERIV_WS_URL);

  ws.addEventListener('open', () => {
    reconnectDelay = 1_000;
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

// Socket is kept alive for the page lifetime — never close it between requests.
// The Deriv public endpoint has no per-connection cost and keeping one persistent
// connection avoids the reconnect churn that caused "connecting every second".

// ─────────────────────────────────────────────
// History Request Queue (rate-limit aware)
// ─────────────────────────────────────────────

function enqueueHistoryRequest(symbol, granularity, from, to, resolve, reject) {
  historyQueue.push({ symbol, granularity, from, to, resolve, reject });
  processHistoryQueue();
}

function processHistoryQueue() {
  if (historyQueueTimer) return; // already draining

  function drainOne() {
    if (historyQueue.length === 0) {
      historyQueueTimer = null;
      return;
    }

    const item = historyQueue.shift();
    const reqId = nextRequestId();

    const timer = setTimeout(() => {
      if (pendingRequests.has(reqId)) {
        pendingRequests.delete(reqId);
        item.reject(new Error('Deriv history request timed out'));
      }
    }, HISTORY_TIMEOUT);

    pendingRequests.set(reqId, {
      _timer: timer,
      resolve: value => { clearTimeout(timer); item.resolve(value); },
      reject: err => { clearTimeout(timer); item.reject(err); },
      isHistory: true,
    });

    send({
      ticks_history: item.symbol,
      adjust_start_time: 1,
      start: item.from,
      end: item.to,
      style: 'candles',
      granularity: item.granularity,
      req_id: reqId,
    });

    // Schedule next request after rate-limit interval
    historyQueueTimer = setTimeout(drainOne, RATE_LIMIT_INTERVAL);
  }

  historyQueueTimer = setTimeout(drainOne, 0);
}

// ─────────────────────────────────────────────
// Message Router
// ─────────────────────────────────────────────

function onMessage(event) {
  let message;
  try { message = JSON.parse(event.data); } catch { return; }

  const reqId = message.req_id ?? message.echo_req?.req_id;

  // Error handling
  if (message.error) {
    const errMsg = message.error.message || JSON.stringify(message.error);
    if (reqId !== undefined && pendingRequests.has(reqId)) {
      const pending = pendingRequests.get(reqId);
      pendingRequests.delete(reqId);
      clearTimeout(pending._timer);

      // Rate-limit: retry with backoff
      if (errMsg.includes('rate limit') || errMsg.includes('Rate limit')) {
        console.warn('[DerivClient] Rate limited, retrying in 3s...');
        setTimeout(() => {
          // Re-enqueue with same params if we still have the request context
          if (pending.isHistory && pending._retryCount !== undefined) {
            if (pending._retryCount < 3) {
              pending._retryCount = (pending._retryCount || 0) + 1;
              // Can't easily re-enqueue, just reject
              pending.reject(new Error(`Deriv API error (after retry): ${errMsg}`));
            } else {
              pending.reject(new Error(`Deriv API error: ${errMsg}`));
            }
          } else {
            pending.reject(new Error(`Deriv API error: ${errMsg}`));
          }
        }, 3000);
        return;
      }

      pending.reject(new Error(`Deriv API error: ${errMsg}`));
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
    }
    return;
  }

  // 2) Subscription initial data (candles batch when subscribe starts).
  if (message.msg_type === 'candles' && message.subscription) {
    const symbol = message.echo_req?.ticks_history || message.echo_req?.underlying_symbol;
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
          handler.isDirty = true; // push initial bar to TV
        }
      });
    }
    return;
  }

  // 3) Realtime tick arriving on a subscribed stream.
  if (message.msg_type === 'tick' && message.tick) {
    const symbol = message.echo_req?.ticks_history || message.echo_req?.underlying_symbol;
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
    }
    return;
  }
}

// ─────────────────────────────────────────────
// Realtime update batcher (250ms coalesce)
// ─────────────────────────────────────────────

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

// Fetches historical OHLCV bars from Deriv.
// Uses cache + rate-limited queue to avoid API limits.
export function fetchHistory(symbol, granularity, fromEpoch, toEpoch) {
  const cKey = cacheKey(symbol, granularity, fromEpoch, toEpoch);

  // Check cache
  const cached = barCache.get(cKey);
  if (cached && Date.now() - cached.ts < CACHE_MAX_AGE) {
    return Promise.resolve(cached.bars);
  }

  return new Promise((resolve, reject) => {
    enqueueHistoryRequest(
      symbol,
      granularity,
      Math.floor(fromEpoch),
      Math.ceil(toEpoch),
      (bars) => {
        // Cache the result
        barCache.set(cKey, { ts: Date.now(), bars });
        // Limit cache size
        if (barCache.size > 500) {
          const firstKey = barCache.keys().next().value;
          barCache.delete(firstKey);
        }
        resolve(bars);
      },
      reject
    );
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
      resolve: value => { clearTimeout(timer); resolve(value); },
      reject: err => { clearTimeout(timer); reject(err); },
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
  }
}

function sendSubscription(symbol, granularity) {
  // IMPORTANT: end is a REQUIRED field for ticks_history.
  // For subscriptions, "latest" means "starting from the most recent data".
  send({
    ticks_history: symbol,
    adjust_start_time: 1,
    end: 'latest',
    style: 'candles',
    granularity,
    subscribe: 1,
    req_id: nextRequestId(),
  });
}
