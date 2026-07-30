import {
  DERIV_EXCHANGE,
  SUPPORTED_RESOLUTIONS,
  DERIV_GRANULARITIES,
  generateSymbol,
  getAllSymbols,
  getSymbolInfoItem,
  parseFullSymbol,
} from './helpers.js';
import {
  fetchHistory,
  fetchServerTime,
  subscribeStream,
  unsubscribeStream,
} from './deriv-client.js';

const lastBarsCache = new Map();

const configurationData = {
  supports_timescale_marks: false,
  supports_marks: false,
  supports_time: true,
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

// Retry helper with exponential backoff
function retry(fn, maxAttempts = 3, baseDelay = 1500) {
  return new Promise((resolve, reject) => {
    function attempt(n) {
      fn()
        .then(resolve)
        .catch(err => {
          if (n >= maxAttempts) {
            reject(err);
          } else {
            const delay = baseDelay * Math.pow(2, n - 1);
            console.warn(
              `[datafeed] Retry attempt ${n}/${maxAttempts} after ${delay}ms:`,
              err.message
            );
            setTimeout(() => attempt(n + 1), delay);
          }
        });
    }
    attempt(1);
  });
}

export default {
  onReady(callback) {
    setTimeout(() => callback(configurationData));
  },

  getServerTime(callback) {
    fetchServerTime()
      .then(epochSeconds => {
        if (Number.isFinite(epochSeconds)) {
          setTimeout(() => callback(epochSeconds));
          return;
        }
        console.warn(
          '[getServerTime] Invalid server time; using local clock.'
        );
        setTimeout(() => callback(Math.floor(Date.now() / 1000)));
      })
      .catch(error => {
        console.warn(
          '[getServerTime] Unable to retrieve server time; using local clock.',
          error
        );
        setTimeout(() => callback(Math.floor(Date.now() / 1000)));
      });
  },

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

    setTimeout(() => onResultReadyCallback(filtered.slice(0, 200)));
  },

  resolveSymbol(
    symbolName,
    onSymbolResolvedCallback,
    onResolveErrorCallback
  ) {
    try {
      const symbolItem = getSymbolInfoItem(symbolName);

      if (!symbolItem) {
        console.warn('[resolveSymbol] Cannot resolve:', symbolName);
        setTimeout(() => onResolveErrorCallback('unknown_symbol'));
        return;
      }

      const generated = generateSymbol(symbolItem.symbol);

      setTimeout(() =>
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
          supported_resolutions:
            configurationData.supported_resolutions,
          data_status: 'streaming',
        })
      );
    } catch (error) {
      console.error('[resolveSymbol] Error:', error);
      setTimeout(() => onResolveErrorCallback('unknown_symbol'));
    }
  },

  async getBars(
    symbolInfo,
    resolution,
    periodParams,
    onHistoryCallback,
    onErrorCallback
  ) {
    const { from, to } = periodParams;

    const parsed = parseFullSymbol(symbolInfo.ticker);
    if (!parsed) {
      setTimeout(() => onErrorCallback('Cannot parse symbol ticker'));
      return;
    }

    const granularity = DERIV_GRANULARITIES[resolution];
    if (!granularity) {
      setTimeout(() =>
        onErrorCallback(`Unsupported resolution: ${resolution}`)
      );
      return;
    }

    const fromSec = Math.floor(from);
    const toSec = Math.ceil(to);

    try {
      // Retry with backoff on failure (rate limits, timeouts)
      const rawBars = await retry(
        () => fetchHistory(parsed.symbol, granularity, fromSec, toSec),
        3,
        1500
      );

      if (!rawBars || rawBars.length === 0) {
        setTimeout(() => onHistoryCallback([], { noData: true }));
        return;
      }

      const bars = rawBars.filter(
        bar => bar.time >= from * 1000 && bar.time < to * 1000
      );

      if (bars.length === 0) {
        setTimeout(() => onHistoryCallback([], { noData: true }));
        return;
      }

      setTimeout(() => onHistoryCallback(bars, { noData: false }));

      // Cache last bar for streaming bootstrap
      const lastBar = bars[bars.length - 1];
      lastBarsCache.set(symbolInfo.ticker, { ...lastBar });
    } catch (error) {
      console.error('[getBars] Error after retries:', error);
      // Don't call onErrorCallback — call onHistoryCallback with noData
      // so TV shows whatever it has instead of breaking with internal errors
      setTimeout(() => onHistoryCallback([], { noData: true }));
    }
  },

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

  unsubscribeBars(subscriberUID) {
    unsubscribeStream(subscriberUID);
  },
};
