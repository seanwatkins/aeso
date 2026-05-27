# AESO Current Supply & Demand → Grafana (InfluxDB) Collector

A Common Lisp program that polls the **AESO Current Supply & Demand (CSD) v2 API**
and writes the results to **InfluxDB**, which Grafana uses as its time-series data source.

---

## Prerequisites

| Tool | Notes |
|------|-------|
| [SBCL](https://www.sbcl.org/) | `sudo apt install sbcl` / `brew install sbcl` |
| [Quicklisp](https://www.quicklisp.org/) | Standard Lisp package manager |
| InfluxDB 2.x | Running locally or in Docker |
| Grafana | Pointed at the same InfluxDB instance |
| AESO API key | Register at https://developer-apim.aeso.ca/ |

---

## Quick Start

### 1. Install SBCL + Quicklisp

```bash
# Debian/Ubuntu
sudo apt install sbcl

# Install Quicklisp
curl -O https://beta.quicklisp.org/quicklisp.lisp
sbcl --load quicklisp.lisp --eval '(quicklisp-quickstart:install)' --quit
```

### 2. Start InfluxDB (Docker example)

```bash
docker run -d --name influxdb \
  -p 8086:8086 \
  -e DOCKER_INFLUXDB_INIT_MODE=setup \
  -e DOCKER_INFLUXDB_INIT_USERNAME=admin \
  -e DOCKER_INFLUXDB_INIT_PASSWORD=password123 \
  -e DOCKER_INFLUXDB_INIT_ORG=my-org \
  -e DOCKER_INFLUXDB_INIT_BUCKET=aeso \
  -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=mytoken \
  influxdb:2
```

### 3. Configure environment variables

```bash
export AESO_API_KEY="your-aeso-api-key"
export INFLUX_URL="http://localhost:8086"
export INFLUX_ORG="my-org"
export INFLUX_BUCKET="aeso"
export INFLUX_TOKEN="mytoken"
```

### 4. Run the collector

```bash
sbcl --load aeso-csd-to-grafana.lisp
```

The program will poll every **60 seconds** by default and print a log line each cycle.

---

## Configuration

All settings are `defparameter` forms near the top of the file and can be overridden
via environment variables:

| Parameter | Env var | Default | Description |
|-----------|---------|---------|-------------|
| `*aeso-api-key*` | `AESO_API_KEY` | — | AESO APIM subscription key |
| `*aeso-csd-url*` | — | CSD v2 endpoint | Change if AESO updates the path |
| `*influx-url*` | `INFLUX_URL` | `http://localhost:8086` | InfluxDB base URL |
| `*influx-org*` | `INFLUX_ORG` | `my-org` | InfluxDB organisation |
| `*influx-bucket*` | `INFLUX_BUCKET` | `aeso` | InfluxDB bucket name |
| `*influx-token*` | `INFLUX_TOKEN` | — | InfluxDB API token |
| `*poll-interval-seconds*` | — | `60` | Set to `NIL` to run once and exit |

---

## Data Written to InfluxDB

### Measurement: `csd_summary`
System-level fields written every cycle:

| Field | Description |
|-------|-------------|
| `alberta_internal_load` | Total Alberta load (MW) |
| `net_to_grid_generation` | Net generation dispatched to grid (MW) |
| `net_interchange` | Net import/export (MW) |
| `actual_forecast` | Forecast vs actual load (MW) |
| `ail_transfer` | AIL transfer (MW) |

### Measurement: `csd_generation`
One row **per fuel type**, tagged with `fuel_type=`:

| Field | Description |
|-------|-------------|
| `mcr` | Maximum continuous rating (MW) |
| `tng` | Total net generation (MW) |
| `dcr` | Dispatched contingency reserve (MW) |

**Fuel types** as of 2026: GAS (COGEN, SIMPLE CYCLE, COMBINED CYCLE), WIND, SOLAR,
HYDRO, ENERGY STORAGE, OTHER.

---

## Grafana Setup

1. Add InfluxDB as a data source in Grafana (Settings → Data Sources → InfluxDB).
2. Use **Flux** query language (InfluxDB v2).

Example Flux query for a dashboard panel:

```flux
from(bucket: "aeso")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "csd_summary")
  |> filter(fn: (r) => r._field == "alberta_internal_load")
```

---

## Running as a System Service (Linux)

Create `/etc/systemd/system/aeso-collector.service`:

```ini
[Unit]
Description=AESO CSD → InfluxDB Collector
After=network.target

[Service]
User=nobody
Environment="AESO_API_KEY=your-key"
Environment="INFLUX_TOKEN=mytoken"
ExecStart=/usr/bin/sbcl --load /opt/aeso-csd-to-grafana.lisp
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now aeso-collector
```

---

## Notes

- **AESO API key required** – register at https://developer-apim.aeso.ca/
- The CSD API is rate-limited; 60-second polling is a safe default.
- Coal and Dual Fuel fuel types were removed by AESO in November 2024.
- The v1 CSD API was retired September 30, 2025; this program uses v2.
