// Realtime quote streams for Trading Platform — disabled for Deriv datafeed.
// Deriv synthetic indices don't have bid/ask/volume quote data like crypto spot markets,
// so this module is a no-op placeholder for now.

import { parseFullSymbol } from './helpers.js';

export function subscribeQuotesOnStream(symbols, listenerGUID, onEvent) {
	// Not implemented — Deriv synthetic indices trade on a single price feed.
}

export function unsubscribeQuotesFromStream(listenerGUID) {
	// Not implemented.
}
