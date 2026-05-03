# Crypto Data Collector

Programme Python pour collecter, stocker et préparer des données crypto utilisables par un bot de trading.

## Structure

- `collector.py` : appels API Binance Futures avec retries, backoff et gestion rate limit.
- `database.py` : base SQLite, table `market_data`, index et upsert anti-duplication.
- `features.py` : RSI, EMA 20/50, ATR, log returns, volatilité rolling, volume spike, open interest delta.
- `main.py` : boucle principale toutes les 5 minutes, logs et arrêt propre.
- `.env.example` : configuration multi-symboles.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Modifier `.env` si besoin :

```env
SYMBOLS=BTCUSDT,ETHUSDT
INTERVAL=5m
LOOP_SECONDS=300
DB_PATH=market_data.sqlite3
LOG_LEVEL=INFO
```

## Lancement

```bash
python main.py
```

La base SQLite est créée automatiquement. Les timestamps sont stockés en UTC sous forme d'epoch seconds. Les doublons sont évités par la clé primaire `(timestamp, symbol)`.

## Notes d'architecture

Binance Futures alimente la table `market_data`. Le collecteur est isolé afin d'ajouter ensuite d'autres sources ou un WebSocket sans refactor majeur.

Pour un VPS, lancer via `systemd`, `supervisor` ou Docker avec redémarrage automatique.
