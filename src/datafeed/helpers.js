// Shared helpers for the Deriv-backed TradingView datafeed.

export const DERIV_EXCHANGE = 'Deriv';

const DAY_MS = 24 * 60 * 60 * 1000;

export const SUPPORTED_RESOLUTIONS = [
	'1',
	'5',
	'15',
	'30',
	'60',
	'120',
	'240',
	'360',
	'480',
	'720',
	'1D',
];

// Maps TradingView resolutions to Deriv candle granularity in seconds.
export const DERIV_GRANULARITIES = {
	'1': 60,
	'5': 300,
	'15': 900,
	'30': 1800,
	'60': 3600,
	'120': 7200,
	'240': 14400,
	'360': 21600,
	'480': 28800,
	'720': 43200,
	'1D': 86400,
};

// Hardcoded symbol catalog for Deriv synthetic indices.
// pip_size = number of decimal places from the Deriv API.
// pricescale is computed as 10^pip_size in resolveSymbol.
const DERIV_SYMBOL_CATALOG = [
  {
    symbol: 'R_10',
    description: 'Volatility 10 Index',
    type: 'synthetic',
    pip_size: 3,
    volumeEnabled: false,
  },
  {
    symbol: 'R_25',
    description: 'Volatility 25 Index',
    type: 'synthetic',
    pip_size: 3,
    volumeEnabled: false,
  },
  {
    symbol: 'R_50',
    description: 'Volatility 50 Index',
    type: 'synthetic',
    pip_size: 3,
    volumeEnabled: false,
  },
  {
    symbol: 'R_75',
    description: 'Volatility 75 Index',
    type: 'synthetic',
    pip_size: 4,
    volumeEnabled: false,
  },
  {
    symbol: 'R_100',
    description: 'Volatility 100 Index',
    type: 'synthetic',
    pip_size: 2,
    volumeEnabled: false,
  },
  {
    symbol: '1HZ10V',
    description: 'Volatility 10 (1s) Index',
    type: 'synthetic',
    pip_size: 2,
    volumeEnabled: false,
  },
  {
    symbol: '1HZ25V',
    description: 'Volatility 25 (1s) Index',
    type: 'synthetic',
    pip_size: 2,
    volumeEnabled: false,
  },
  {
    symbol: '1HZ50V',
    description: 'Volatility 50 (1s) Index',
    type: 'synthetic',
    pip_size: 2,
    volumeEnabled: false,
  },
  {
    symbol: '1HZ75V',
    description: 'Volatility 75 (1s) Index',
    type: 'synthetic',
    pip_size: 2,
    volumeEnabled: false,
  },
  {
    symbol: '1HZ100V',
    description: 'Volatility 100 (1s) Index',
    type: 'synthetic',
    pip_size: 2,
    volumeEnabled: false,
  },
  {
    symbol: 'BOOM300N',
    description: 'Boom 300 Index',
    type: 'synthetic',
    pip_size: 2,
    volumeEnabled: false,
  },
  {
    symbol: 'CRASH300N',
    description: 'Crash 300 Index',
    type: 'synthetic',
    pip_size: 2,
    volumeEnabled: false,
  },
];

// Builds the symbol shapes used by TradingView search and resolve flows.
export function generateSymbol(symbolName) {
	const short = symbolName;
	const full = `${DERIV_EXCHANGE}:${symbolName}`;

	return { short, full, symbol: symbolName };
}

// Rounds a timestamp down to the opening time of its containing bar.
export function barStartTime(timestampMs, resolution) {
	const granularitySec = DERIV_GRANULARITIES[resolution];
	if (!granularitySec) return timestampMs;

	if (resolution === '1D') {
		const d = new Date(timestampMs);
		d.setUTCHours(0, 0, 0, 0);
		return d.getTime();
	}

	const intervalMs = granularitySec * 1000;
	return Math.floor(timestampMs / intervalMs) * intervalMs;
}

// Advances a bar timestamp to the next bar boundary for the same resolution.
export function getNextBarTime(barTimeMs, resolution) {
	const granularitySec = DERIV_GRANULARITIES[resolution];
	if (!granularitySec) return barTimeMs;

	const intervalMs = granularitySec * 1000;
	return barTimeMs + intervalMs;
}

// Maps a TradingView resolution to its Deriv granularity in seconds.
export function getResolutionSpec(resolution) {
	const granularity = DERIV_GRANULARITIES[resolution];
	if (!granularity) return null;

	return { granularity };
}

// Finds a symbol regardless of whether the library passes short or full ticker text.
export function getSymbolInfoItem(symbolName) {
	const needle = symbolName.toLowerCase().replace(/^deriv:/, '');

	return (
		DERIV_SYMBOL_CATALOG.find(
			item => item.symbol.toLowerCase() === needle
		) ?? null
	);
}

// Exposes the full catalog for search.
export function getAllSymbols() {
	return DERIV_SYMBOL_CATALOG.map(item => {
		const generated = generateSymbol(item.symbol);
		return {
			symbol: generated.short,
			full_name: generated.full,
			ticker: generated.full,
			description: item.description,
			exchange: DERIV_EXCHANGE,
			type: item.type,
		};
	});
}

// Strips the exchange prefix from a TradingView ticker (e.g. "Deriv:R_10" → "R_10").
export function parseFullSymbol(fullSymbol) {
	const match = fullSymbol.match(/^(?:[^:]+:)?(.+)$/);
	if (!match) return null;

	return {
		symbol: match[1].toUpperCase(),
	};
}
