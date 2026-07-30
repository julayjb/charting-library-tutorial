// Deriv realtime streaming bridge — routes WebSocket candle updates to chart subscribers.
// The actual stream management lives in deriv-client.js; this file re-exports the
// subscribeOnStream / unsubscribeFromStream API that datafeed.js expects.

import {
	subscribeStream,
	unsubscribeStream,
} from './deriv-client.js';
import { DERIV_GRANULARITIES, parseFullSymbol } from './helpers.js';

// Subscribes a chart listener to the Deriv stream that powers its resolution.
export function subscribeOnStream(
	symbolInfo,
	resolution,
	onRealtimeCallback,
	subscriberUID,
	onResetCacheNeededCallback,
	lastBar
) {
	if (!symbolInfo?.ticker) {
		console.error('[subscribeOnStream] Invalid symbolInfo:', symbolInfo);
		return;
	}

	const parsed = parseFullSymbol(symbolInfo.ticker);
	if (!parsed) {
		console.error(
			'[subscribeOnStream] Cannot parse ticker:',
			symbolInfo.ticker
		);
		return;
	}

	const granularity = DERIV_GRANULARITIES[resolution];
	if (!granularity) {
		console.error(
			'[subscribeOnStream] Unsupported resolution:',
			resolution
		);
		return;
	}

	subscribeStream(
		parsed.symbol,
		granularity,
		onRealtimeCallback,
		subscriberUID,
		lastBar ?? null
	);
}

// Removes a chart subscriber and closes the stream when nobody is left on it.
export function unsubscribeFromStream(subscriberUID) {
	unsubscribeStream(subscriberUID);
}
