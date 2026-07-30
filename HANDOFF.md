# Handoff — TradingView Advanced Charts + Deriv Datafeed

**Data:** 30/Jul/2026  
**Repositório:** `github.com/julayjb/charting-library-tutorial`  
**Branch:** `feat/deriv-datafeed`  
**Responsável:** Julay PontoBots (@JulayPontoBots)

---

## 1. Visão Geral

Arquitetura de integração entre a **TradingView Advanced Charts** (v31+) e a **API pública de dados da Deriv**, permitindo exibir gráficos de velas em tempo real dos índices sintéticos da Deriv.

### Fluxo de dados

```
TradingView Advanced Charts (chart)
  ↓ 6 métodos do datafeed
Datafeed Adapter (datafeed.js)
  ↓ 4 funções exportadas
Deriv WebSocket Client (deriv-client.js)
  ↓ WebSocket (sem auth)
wss://api.derivws.com/trading/v1/options/ws/public
  ↓
Deriv API — Market Data Pública
```

### Tecnologias

- **TradingView Advanced Charts** v31.2+ (licenciado)
- **WebSocket nativo** (sem bibliotecas externas)
- **ES Modules** (vanilla JS, sem bundler específico)
- **Dados públicos** — sem autenticação, sem OTP, sem tokens

---

## 2. Estrutura de Arquivos

```
src/
├── datafeed/
│   ├── deriv-client.js    ← Cliente WebSocket (socket, fila, cache)
│   ├── datafeed.js        ← Adapter TV → Deriv (6 métodos obrigatórios)
│   └── helpers.js         ← Catálogo de symbols, resoluções, utilitários
├── widget-options.js      ← Configuração do widget TV (ponto de entrada)
├── trading.js             ← Página de exemplo que monta o chart
└── theme.js               ← Tema e custom CSS
```

---

## 3. Deriv WebSocket Client (`deriv-client.js`)

### 3.1 Endpoint

```
wss://api.derivws.com/trading/v1/options/ws/public
```

- **Público** — sem autenticação, sem app_id, sem OTP
- **Acesso somente a dados de mercado** (sem trading, sem saldo)
- Suporta: `ticks_history`, `ticks`, `time`, `active_symbols`

### 3.2 Gerenciamento do Socket

| Característica | Implementação |
|---|---|
| **Criação** | Sob demanda — primeira chamada a `send()` cria o socket |
| **Persistência** | Socket vive pela vida inteira da página (nunca é fechado entre requests) |
| **Message Queue** | Mensagens enviadas antes do `open` são enfileiradas e descarregadas no evento `open` |
| **Reconexão** | Backoff exponencial (1s → 2s → 4s → ... → 30s max) só quando há subscriptions ativas |
| **Pool único** | Uma única conexão WebSocket para todas as requests e subscriptions |

### 3.3 Request Queue (Rate Limit)

A Deriv tem rate limit em `ticks_history`. O sistema usa uma **fila com throttle**:

- Máximo de **1 chamada `ticks_history` a cada 1.2s**
- As requests são enfileiradas e processadas uma por vez
- Cache LRU de bars com TTL de **5 minutos** evita re-requests do mesmo período

### 3.4 API Exportada

| Função | Parâmetros | Retorno | Descrição |
|---|---|---|---|
| `fetchHistory(symbol, granularity, from, to)` | `string, number, epoch, epoch` | `Promise<Bar[]>` | Histórico de velas (usa fila rate-limited + cache) |
| `fetchServerTime()` | — | `Promise<number>` | Hora do servidor Deriv (epoch em segundos) |
| `subscribeStream(symbol, granularity, callback, uid, lastBar)` | `string, number, fn, string, Bar?` | `void` | Assina streaming de OHLC em tempo real |
| `unsubscribeStream(uid)` | `string` | `void` | Cancela subscription |

### 3.5 Formato dos dados (Bar)

```javascript
{
  time: 1785439764000,     // epoch em ms
  open: 4964.853,
  high: 4966.756,
  low: 4963.938,
  close: 4964.078
}
```

### 3.6 Roteamento de Mensagens

O `onMessage` roteia por `msg_type`:

| `msg_type` | Origem | Ação |
|---|---|---|
| `candles` + sem `subscription` | `fetchHistory` (one-shot) | Resolve promise com array de bars |
| `candles` + com `subscription` | `subscribeStream` (batch inicial) | Initializa handler com última vela |
| `tick` | Subscription (tick a tick) | Atualiza última vela (abre nova se mudou de período) |
| `ohlc` | Subscription (candle pronto) | Substitui última vela |
| `time` | `fetchServerTime` | Resolve promise com hora do servidor |

---

## 4. TV Datafeed Adapter (`datafeed.js`)

### 4.1 Métodos Obrigatórios da TV

| Método | Descrição | Detalhes |
|---|---|---|
| `onReady(callback)` | Configuração inicial | Chama `callback(configurationData)` via `setTimeout(0)` |
| `resolveSymbol(name, onResolved, onError)` | Resolve symbol info | Busca no catálogo, retorna `pricescale`, `session: '24x7'`, etc |
| `getBars(symbolInfo, resolution, periodParams, onHistory, onError)` | Dados históricos | ~`fetchHistory` com retry 3x + fila rate-limited |
| `subscribeBars(symbolInfo, resolution, onRealtime, uid, onReset)` | Streaming realtime | ~`subscribeStream` com último bar do cache |
| `unsubscribeBars(uid)` | Parar streaming | ~`unsubscribeStream` |
| `getServerTime(callback)` | Sincronizar relógio | ~`fetchServerTime`, fallback para `Date.now()/1000` |
| `searchSymbols(query, exchange, type, onResult)` | Busca de symbols | Filtro no catálogo local |

### 4.2 Regra Crítica: `setTimeout(0)`

**TODO** callback da TV DEVE ser chamado assincronamente. A Advanced Charts v31+ detecta se o callback é síncrono e rejeita com:

```
`resolveSymbol` should return result asynchronously.
```

**Sempre usar:**
```javascript
setTimeout(() => onSymbolResolvedCallback({ ... }));
setTimeout(() => onHistoryCallback(bars, { noData: false }));
setTimeout(() => callback(configurationData));
// ... todos os callbacks
```

### 4.3 Tratamento de Erros

Quando `getBars` falha (rate limit, timeout), chamar `onHistoryCallback([], { noData: true })` em vez de `onErrorCallback` — isso evita erros internos da TV (`toLowerCase` / `startsWith`).

```javascript
// ❌ NÃO fazer:
onErrorCallback('rate limited');

// ✅ FAZER:
setTimeout(() => onHistoryCallback([], { noData: true }));
```

---

## 5. Catálogo de Symbols (`helpers.js`)

### 5.1 Resoluções Suportadas

```javascript
['1', '5', '15', '30', '60', '120', '240', '360', '480', '720', '1D']
```

### 5.2 Mapeamento TV → Deriv

| Resolução TV | Granularidade Deriv (segundos) |
|---|---|
| `1` | 60 |
| `5` | 300 |
| `15` | 900 |
| `30` | 1800 |
| `60` | 3600 |
| `120` | 7200 |
| `240` | 14400 |
| `360` | 21600 |
| `480` | 28800 |
| `720` | 43200 |
| `1D` | 86400 |

### 5.3 Symbols e `pip_size`

| Symbol | Descrição | `pip_size` | `pricescale` |
|---|---|---|---|
| `R_10` | Volatility 10 Index | 3 | 1000 |
| `R_25` | Volatility 25 Index | 3 | 1000 |
| `R_50` | Volatility 50 Index | 3 | 1000 |
| `R_75` | Volatility 75 Index | 4 | 10000 |
| `R_100` | Volatility 100 Index | 2 | 100 |
| `1HZ10V` | Volatility 10 (1s) Index | 2 | 100 |
| `1HZ25V` | Volatility 25 (1s) Index | 2 | 100 |
| `1HZ50V` | Volatility 50 (1s) Index | 2 | 100 |
| `1HZ75V` | Volatility 75 (1s) Index | 2 | 100 |
| `1HZ100V` | Volatility 100 (1s) Index | 2 | 100 |
| `BOOM300N` | Boom 300 Index | 2 | 100 |
| `CRASH300N` | Crash 300 Index | 2 | 100 |

O `pricescale` é calculado como `10^pip_size` no `resolveSymbol`.

### 5.4 Formato do Ticker

```
Deriv:R_10
Deriv:R_75
Deriv:1HZ10V
```

O prefixo `Deriv:` é opcional — a busca aceita tanto `R_10` quanto `Deriv:R_10`.

---

## 6. Configuração do Widget (`widget-options.js`)

### 6.1 Parâmetros Principais

```javascript
{
  symbol: 'Deriv:R_10',        // símbolo inicial
  interval: '5',               // resolução inicial (5 minutos)
  container: 'tv_chart_container',
  datafeed: datafeed,          // instância do datafeed
  library_path: 'vendor/tradingview/advanced_charts/',
  locale: 'en',
  fullscreen: true,
  symbol_search_request_delay: 1000,
  theme: 'Dark',
  enabled_features: [
    'custom_resolutions',
    'allow_arbitrary_symbol_search_input',
    'display_data_mode',
    'chart_drag_export',
  ],
  disabled_features: [
    'use_localstorage_for_settings',
    'save_chart_properties_to_local_storage',
    'volume_force_overlay',
  ],
}
```

### 6.2 Datafeed da Free Edition

Na versão gratuita da Advanced Charts (sem Trading Platform), remover os métodos não suportados:

```javascript
delete clone.getQuotes;
delete clone.subscribeQuotes;
delete clone.unsubscribeQuotes;
delete clone.subscribeDepth;
delete clone.unsubscribeDepth;
```

---

## 7. Passo a Passo para Reproduzir (do Zero)

### 7.1 Setup do Projeto

```bash
# 1. Criar projeto (ou usar o charting-library-tutorial como base)
npm init

# 2. Instalar a TradingView Advanced Charts
npm install --save @tradingview/charting_library

# 3. Estrutura de pastas
mkdir -p src/datafeed
mkdir -p public/vendor/tradingview/advanced_charts
cp -r node_modules/@tradingview/charting_library/* public/vendor/tradingview/advanced_charts/
```

### 7.2 Criar os Arquivos (em ordem)

```
src/
├── datafeed/
│   ├── helpers.js         ← Copiar deste handoff
│   ├── deriv-client.js    ← Copiar deste handoff
│   └── datafeed.js        ← Copiar deste handoff
├── widget-options.js      ← Copiar deste handoff
└── trading.js             ← Página de exemplo
```

### 7.3 Verificações Obrigatórias

- [ ] **`setTimeout(0)`** em todos os callbacks da TV (`resolveSymbol`, `getBars`, `onReady`, `searchSymbols`, `getServerTime`)
- [ ] **`end: "latest"`** na subscription (`ticks_history` + `subscribe: 1`)
- [ ] **`end: <epoch>`** no histórico (`ticks_history` one-shot)
- [ ] **Socket persistente** — NUNCA fechar o socket entre requests
- [ ] **Message queue** — enfileirar mensagens enquanto socket está CONNECTING
- [ ] **Rate limit** — throttle de 1.2s entre `ticks_history`
- [ ] **Cache** — LRU com TTL de 5 min para bars históricas
- [ ] **Retry** — 3 tentativas com backoff em caso de rate limit/timeout
- [ ] **`pricescale` dinâmico** — `10^pip_size` no resolveSymbol
- [ ] **Fallback de horário** — se `getServerTime` falhar, usar `Date.now() / 1000`

### 7.4 Testar

```bash
npm run start
# Abrir http://localhost:3000
# Verificar no console:
#   ✅ WebSocket único conectado
#   ✅ Dados históricos carregando
#   ✅ OHLC streaming em tempo real
#   ✅ Troca de símbolo sem rate limit
#   ✅ Troca de resolução sem rate limit
#   ✅ Casas decimais corretas por symbol
```

---

## 8. API de Referência

### Deriv Public WebSocket

| Ação | Mensagem Request | Resposta |
|---|---|---|
| Histórico de velas | `{ ticks_history: "R_10", start: <epoch>, end: <epoch>, style: "candles", granularity: 300 }` | `{ msg_type: "candles", candles: [...] }` |
| Subscription tempo real | `{ ticks_history: "R_10", end: "latest", style: "candles", granularity: 300, subscribe: 1 }` | Inicial: `{ msg_type: "candles", candles: [...], subscription: { id } }` → Streaming: `{ msg_type: "ohlc", ohlc: {...} }` |
| Hora do servidor | `{ time: 1 }` | `{ msg_type: "time", time: <epoch> }` |
| Cancelar subscription | `{ forget: "<subscription_id>" }` | `{ msg_type: "forget" }` |

### Docs Oficiais

- https://developers.deriv.com/llms/ticks-history.md
- https://developers.deriv.com/llms/ticks.md
- https://developers.deriv.com/llms/time.md
- https://developers.deriv.com/llms/forget.md

---

## 9. Observações para Produção

### Quando migrar para o PontoBots oficial

1. **Copiar os 3 arquivos** da pasta `src/datafeed/` para o projeto PontoBots
2. **Ajustar o path da library_path** para onde a Advanced Charts estiver hospedada
3. **Verificar símbolos** — se o catálogo precisar incluir mais mercados, adicionar no `DERIV_SYMBOL_CATALOG` com o `pip_size` correto
4. **Se usar autenticação** (para trading), o WebSocket muda para:
   ```
   wss://api.derivws.com/trading/v1/options/ws/demo
   wss://api.derivws.com/trading/v1/options/ws/real
   ```
   Mas para **dados de gráfico apenas**, o endpoint público é suficiente
5. **Rate limits** — os sintéticos são generosos, mas se houver muitos charts abertos simultaneamente, considerar aumentar o `RATE_LIMIT_INTERVAL`

---

*Documento gerado em 30/Jul/2026 — branch `feat/deriv-datafeed` do repositório `charting-library-tutorial`.*
