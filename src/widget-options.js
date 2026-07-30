import Datafeed from './datafeed/datafeed.js';
import { cssBlobUrl, getChartOverrides, theme } from './theme.js';

const SHARED_ENABLED_FEATURES = [
	'custom_resolutions',
	'allow_arbitrary_symbol_search_input',
	'display_data_mode',
	'use_symbol_name_for_header_toolbar',
	'chart_drag_export',
];

const SHARED_DISABLED_FEATURES = [
	'use_localstorage_for_settings',
	'save_chart_properties_to_local_storage',
	'volume_force_overlay',
];

// The free Advanced Charts page does not have widgetbar quote/news/DOM UI, so expose
// only the chart datafeed methods it can use.
function createAdvancedChartsDatafeed(datafeed) {
	const clone = { ...datafeed };

	delete clone.getQuotes;
	delete clone.subscribeQuotes;
	delete clone.unsubscribeQuotes;
	delete clone.subscribeDepth;
	delete clone.unsubscribeDepth;

	return clone;
}

const ADVANCED_CHARTS_DATAFEED = createAdvancedChartsDatafeed(Datafeed);

// Deduplicates feature flags after individual pages add their own options.
function unique(values) {
	return [...new Set(values)];
}

// Builds the common widget constructor payload used by the minimal and trading pages.
export function createWidgetOptions({
	datafeed = Datafeed,
	enabledFeatures = [],
	disabledFeatures = [],
	chartOverrides = {},
	libraryPath = 'vendor/tradingview/advanced_charts/',
	...options
} = {}) {
	return {
		symbol: 'Deriv:R_10',
		interval: '5',
		fullscreen: true,
		container: 'tv_chart_container',
		datafeed,
		library_path: libraryPath,
		locale: 'en',
		symbol_search_request_delay: 1000,
		theme,
		custom_css_url: cssBlobUrl,
		custom_font_family: "'NanumBarunGothic', sans-serif",
		enabled_features: unique([
			...SHARED_ENABLED_FEATURES,
			...enabledFeatures,
		]),
		disabled_features: unique([
			...SHARED_DISABLED_FEATURES,
			...disabledFeatures,
		]),
		overrides: {
			...getChartOverrides(theme),
			...chartOverrides,
		},
		...options,
	};
}

// Builds the regular Advanced Charts experience without Trading Platform-only options.
export function createAdvancedChartOptions({ ...options } = {}) {
	return createWidgetOptions({
		...options,
		datafeed: ADVANCED_CHARTS_DATAFEED,
		libraryPath: 'vendor/tradingview/advanced_charts/',
	});
}
