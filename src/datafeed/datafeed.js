import {
	DERIV_EXCHANGE,
	SUPPORTED_RESOLUTIONS,
	DERIV_GRANULARITIES,
	generateSymbol,
	getAllSymbols,
	getSymbolInfoItem,
	parseFullSymbol,
	barStartTime,
} from './helpers.js';
import {
	fetchHistory,
	subscribeStream,
	unsubscribeStream,
} from './deriv-client.js';

const lastBarsCache = new Map();

const configurationData = {
	supports_timescale_marks: false,
	supports_marks: false,
	supports_time: false,
	supported_resolutions: SUPPORTED_RESOLUTIONS,
	exchanges: [
		{
			value: DERIV_EXCHANGE,
			name: DERIV_EXCHANGE,
			desc: 'Deriv synthetic indices',
		},
	],
	symbols_types: [{ name: 'synthetic', value: 'synthetic' }],
};

export default {
	// Publishes the datafeed capabilities TradingView uses during startup.
	onReady(callback) {
		setTimeout(() => callback(configurationData));
	},

	// Returns search matches from the hardcoded Deriv symbol catalog.
	async searchSymbols(
		userInput,
		exchange,
		symbolType,
		onResultReadyCallback
	) {
		const symbols = getAllSymbols();
		const query = userInput.trim().toLowerCase();

		const filtered = symbols.filter(symbol => {
			const matchesExchange =
				!exchange || symbol.exchange === exchange;
			const matchesType =
				!symbolType || symbol.type === symbolType;
			const matchesQuery =
				!query ||
				symbol.ticker.toLowerCase().includes(query) ||
				symbol.symbol.toLowerCase().includes(query) ||
				symbol.description.toLowerCase().includes(query);

			return matchesExchange && matchesType && matchesQuery;
		});

		onResultReadyCallback(filtered.slice(0, 200));
	},

	// Resolves a TradingView ticker into symbol metadata for Deriv synthetic indices.
	async resolveSymbol(
		symbolName,
		onSymbolResolvedCallback,
		onResolveErrorCallback
	) {
		try {
			const symbolItem = getSymbolInfoItem(symbolName);

			if (!symbolItem) {
				console.warn('[resolveSymbol] Cannot resolve:', symbolName);
				onResolveErrorCallback('unknown_symbol');
				return;
			}

			const generated = generateSymbol(symbolItem.symbol);

			onSymbolResolvedCallback({
				ticker: generated.full,
				name: generated.short,
				description: symbolItem.description,
				type: symbolItem.type,
				exchange: DERIV_EXCHANGE,
				listed_exchange: DERIV_EXCHANGE,
				session: '24x7',
				logo_urls: [],
				timezone: 'Etc/UTC',
				minmov: 1,
				pricescale: symbolItem.pricescale,
				format: 'price',
				has_intraday: true,
				intraday_multipliers: SUPPORTED_RESOLUTIONS.filter(
					r => r !== '1D'
				),
				has_daily: true,
				daily_multipliers: ['1'],
				has_weekly_and_monthly: true,
				visible_plots_set: 'ohlcv',
				supported_resolutions: configurationData.supported_resolutions,
				data_status: 'streaming',
			});
		} catch (error) {
			console.error('[resolveSymbol] Error:', error);
			onResolveErrorCallback('unknown_symbol');
		}
	},

	// Fetches historical bars from Deriv ticks_history API with pagination.
	async getBars(
		symbolInfo,
		resolution,
		periodParams,
		onHistoryCallback,
		onErrorCallback
	) {
		const { from, to, firstDataRequest } = periodParams;

		const parsed = parseFullSymbol(symbolInfo.ticker);
		if (!parsed) {
			onErrorCallback('Cannot parse symbol ticker');
			return;
		}

		const granularity = DERIV_GRANULARITIES[resolution];
		if (!granularity) {
			onErrorCallback(`Unsupported resolution: ${resolution}`);
			return;
		}

		// Deriv ticks_history uses epoch in seconds.
		const fromSec = Math.floor(from);
		const toSec = Math.ceil(to);

		try {
			const rawBars = await fetchHistory(
				parsed.symbol,
				granularity,
				fromSec,
				toSec
			);

			if (!rawBars || rawBars.length === 0) {
				onHistoryCallback([], { noData: true });
				return;
			}

			// Filter to requested range.
			const bars = rawBars.filter(
				bar => bar.time >= from * 1000 && bar.time < to * 1000
			);

			if (bars.length === 0) {
				onHistoryCallback([], { noData: true });
				return;
			}

			if (firstDataRequest) {
				lastBarsCache.set(symbolInfo.ticker, {
					...bars[bars.length - 1],
				});
			}

			onHistoryCallback(bars, { noData: false });
		} catch (error) {
			console.error('[getBars] Error:', error);
			onErrorCallback(error);
		}
	},

	// Starts the realtime stream for the active chart symbol and resolution.
	subscribeBars(
		symbolInfo,
		resolution,
		onRealtimeCallback,
		subscriberUID,
		onResetCacheNeededCallback
	) {
		const parsed = parseFullSymbol(symbolInfo.ticker);
		if (!parsed) {
			console.error(
				'[subscribeBars] Cannot parse ticker:',
				symbolInfo.ticker
			);
			return;
		}

		const granularity = DERIV_GRANULARITIES[resolution];
		if (!granularity) {
			console.error(
				'[subscribeBars] Unsupported resolution:',
				resolution
			);
			return;
		}

		subscribeStream(
			parsed.symbol,
			granularity,
			onRealtimeCallback,
			subscriberUID,
			lastBarsCache.get(symbolInfo.ticker) ?? null
		);
	},

	// Stops the realtime bar stream when TradingView releases a subscriber.
	unsubscribeBars(subscriberUID) {
		unsubscribeStream(subscriberUID);
	},
};
