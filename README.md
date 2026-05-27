# AESO CSD + SMP → InfluxDB/Grafana Collector

> **This code was written by [Claude](https://claude.ai) (Anthropic), an AI assistant,
> through an iterative conversation with a human operator who provided the requirements,
> tested each change against live systems, and guided development through real error output
> and debugging sessions. Every bug fix, API path discovery, SSL workaround, and MQTT
> implementation was developed collaboratively — the human ran the code, the AI wrote it.**

A Common Lisp program that polls the **AESO Current Supply & Demand (CSD)** and
**System Marginal Price (SMP)** APIs every 60 seconds, writes metrics to **InfluxDB**,
and publishes status notifications to an **MQTT broker**.

---

## Features

- Real-time Alberta grid generation data per asset (200+ generators)
- System Marginal Price ($/MWh) every 60 seconds
- Persistent MQTT connection with heartbeat, poll notices, and error alerts
- Automatic DNS retry on transient network failures
- OpenSSL 3.x compatibility (Azure APIM close_notify workaround)
- Configurable via environment variables

---

## Prerequisites

| Tool | Notes |
|------|-------|
| [SBCL](https://www.sbcl.org/) | `sudo apt install sbcl` |
| [Quicklisp](https://www.quicklisp.org/) | Standard Lisp package manager |
| InfluxDB 2.x | Running locally or in Docker |
| Grafana | Pointed at the same InfluxDB instance |
| MQTT broker | Mosquitto or any MQTT 3.1.1 broker |
| AESO API key | Register at https://developer-apim.aeso.ca/ |

### Quicklisp dependencies (auto-installed)

`dexador` `yason` `local-time` `usocket` `cl+ssl` `babel`

---

## Quick Start

### 1. Install SBCL + Quicklisp

```bash
sudo apt install sbcl

curl -O https://beta.quicklisp.org/quicklisp.lisp
sbcl --load quicklisp.lisp --eval '(quicklisp-quickstart:install)' --quit
```

### 2. Start InfluxDB (Docker)

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
export MQTT_HOST="your-mqtt-broker-ip"
export MQTT_PORT="1883"
export MQTT_USER="your-mqtt-username"
export MQTT_PASS="your-mqtt-password"
```

### 4. Run

```bash
sbcl --load aeso-csd-to-grafana.lisp
```

---

## Configuration

All settings are `defparameter` forms near the top of the file and can be overridden
via environment variables:

| Parameter | Env var | Default | Description |
|-----------|---------|---------|-------------|
| `*aeso-api-key*` | `AESO_API_KEY` | — | AESO APIM subscription key |
| `*aeso-csd-url*` | — | CSD v1 endpoint | Change if AESO updates the path |
| `*aeso-smp-url*` | — | SMP v1.1 endpoint | Change if AESO updates the path |
| `*influx-url*` | `INFLUX_URL` | `http://localhost:8086` | InfluxDB base URL |
| `*influx-org*` | `INFLUX_ORG` | `my-org` | InfluxDB organisation |
| `*influx-bucket*` | `INFLUX_BUCKET` | `aeso` | InfluxDB bucket name |
| `*influx-token*` | `INFLUX_TOKEN` | — | InfluxDB API token |
| `*mqtt-host*` | `MQTT_HOST` | `localhost` | MQTT broker hostname or IP |
| `*mqtt-port*` | `MQTT_PORT` | `1883` | MQTT broker port |
| `*mqtt-username*` | `MQTT_USER` | `""` | MQTT username (empty = no auth) |
| `*mqtt-password*` | `MQTT_PASS` | `""` | MQTT password |
| `*mqtt-topic-prefix*` | — | `aeso/collector` | Base MQTT topic prefix |
| `*poll-interval-seconds*` | — | `60` | Set to `NIL` to run once and exit |
| `*dns-retry-attempts*` | — | `3` | Retries on DNS failure per poll |
| `*dns-retry-delay*` | — | `5` | Seconds between DNS retries |
| `*debug-json*` | — | `nil` | Set to `T` to print raw JSON keys |

---

## AESO API Endpoints

| Endpoint | Path |
|----------|------|
| CSD assets | `https://apimgw.aeso.ca/public/currentsupplydemand-api/v1/csd/generation/assets/current` |
| System Marginal Price | `https://apimgw.aeso.ca/public/systemmarginalprice-api/v1.1/price/systemMarginalPrice/current` |

Both use the `API-KEY` header with your APIM subscription key.

---

## Data Written to InfluxDB

### Measurement: `csd_asset`

One row per generator per poll cycle (~200 assets). Tags and fields:

| Tag | Description |
|-----|-------------|
| `asset` | Asset ID (e.g. `GNR1`, `BSR1`) |
| `fuel_type` | `GAS`, `WIND`, `SOLAR`, `HYDRO`, `ENERGY STORAGE`, `OTHER` |
| `sub_fuel_type` | `COMBINED_CYCLE`, `COGENERATION`, `SIMPLE_CYCLE`, `GAS_FIRED_STEAM`, `NONE` |

| Field | Description |
|-------|-------------|
| `net_generation` | Current generation (MW) |
| `maximum_capability` | Rated capacity (MW) |
| `dispatched_contingency_reserve` | DCR held (MW) |

### Measurement: `aeso_smp`

One row per poll cycle.

| Field | Description |
|-------|-------------|
| `system_marginal_price` | Current SMP ($/MWh) |
| `volume` | Volume at marginal offer (MW) |

---

## MQTT Topics

The collector maintains a **persistent TCP connection** to the broker and publishes:

| Topic | When | Payload fields |
|-------|------|---------------|
| `aeso/collector/status` | Startup + shutdown | `status`, `timestamp` |
| `aeso/collector/heartbeat` | Every **1 second** | `unix_time`, `uptime_sec`, `uptime`, `failures`, `timestamp` |
| `aeso/collector/poll` | Every successful 60s poll | `status`, `assets`, `system_marginal_price`, `consecutive_failures`, `timestamp` |
| `aeso/collector/error` | Any fetch or write failure | `reason`, `details`, `consecutive_failures`, `timestamp` |

### Home Assistant MQTT sensors

```yaml
mqtt:
  sensor:
    - name: "Alberta SMP"
      state_topic: "aeso/collector/poll"
      value_template: "{{ value_json.system_marginal_price }}"
      unit_of_measurement: "$/MWh"

    - name: "AESO Collector Uptime"
      state_topic: "aeso/collector/heartbeat"
      value_template: "{{ value_json.uptime }}"

    - name: "AESO Collector Failures"
      state_topic: "aeso/collector/heartbeat"
      value_template: "{{ value_json.failures }}"
```

---

## Grafana Queries

### Generation by fuel type (time series)

```flux
from(bucket: "aeso")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "csd_asset")
  |> filter(fn: (r) => r._field == "net_generation")
  |> group(columns: ["fuel_type"])
  |> aggregateWindow(every: v.windowPeriod, fn: sum, createEmpty: false)
  |> map(fn: (r) => ({_time: r._time, _value: r._value, _field: r.fuel_type}))
  |> group()
```

### System Marginal Price (time series)

```flux
from(bucket: "aeso")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "aeso_smp")
  |> filter(fn: (r) => r._field == "system_marginal_price")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
```

### SMP + total generation (dual axis)

Query A — left axis, MW:
```flux
from(bucket: "aeso")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "csd_asset")
  |> filter(fn: (r) => r._field == "net_generation")
  |> group()
  |> aggregateWindow(every: v.windowPeriod, fn: sum, createEmpty: false)
```

Query B — right axis, $/MWh:
```flux
from(bucket: "aeso")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "aeso_smp")
  |> filter(fn: (r) => r._field == "system_marginal_price")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
```

### Total generation stat panel

```flux
from(bucket: "aeso")
  |> range(start: -2m)
  |> filter(fn: (r) => r._measurement == "csd_asset")
  |> filter(fn: (r) => r._field == "net_generation")
  |> last()
  |> group()
  |> sum()
```

### Per-asset table

```flux
from(bucket: "aeso")
  |> range(start: -2m)
  |> filter(fn: (r) => r._measurement == "csd_asset")
  |> last()
  |> group()
  |> pivot(rowKey: ["asset", "fuel_type", "sub_fuel_type"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["asset", "fuel_type", "sub_fuel_type", "net_generation", "maximum_capability", "dispatched_contingency_reserve"])
  |> sort(columns: ["net_generation"], desc: true)
```

### Capacity utilization % by fuel type

```flux
from(bucket: "aeso")
  |> range(start: -2m)
  |> filter(fn: (r) => r._measurement == "csd_asset")
  |> last()
  |> group()
  |> pivot(rowKey: ["asset", "fuel_type"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["fuel_type"])
  |> reduce(
      identity: {tng: 0.0, mcr: 0.0},
      fn: (r, accumulator) => ({
        tng: accumulator.tng + float(v: r.net_generation),
        mcr: accumulator.mcr + float(v: r.maximum_capability)
      })
    )
  |> map(fn: (r) => ({r with utilization_pct: r.tng / r.mcr * 100.0}))
  |> keep(columns: ["fuel_type", "utilization_pct"])
```

### Generation heatmap by fuel type

```flux
import "date"

from(bucket: "aeso")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "csd_asset")
  |> filter(fn: (r) => r._field == "net_generation")
  |> group(columns: ["fuel_type"])
  |> aggregateWindow(every: 1h, fn: sum, createEmpty: false)
  |> drop(columns: ["_start", "_stop", "_measurement", "asset", "sub_fuel_type"])
```

---

## Running as a System Service (Linux)

Create `/etc/systemd/system/aeso-collector.service`:

```ini
[Unit]
Description=AESO CSD + SMP → InfluxDB Collector
After=network.target

[Service]
User=nobody
Environment="AESO_API_KEY=your-key"
Environment="INFLUX_TOKEN=mytoken"
Environment="MQTT_HOST=your-broker-ip"
Environment="MQTT_USER=your-mqtt-user"
Environment="MQTT_PASS=your-mqtt-pass"
ExecStart=/usr/bin/sbcl --load /opt/aeso-csd-to-grafana.lisp
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now aeso-collector
sudo journalctl -u aeso-collector -f
```

---

## Notes

- **AESO API key** — register at https://developer-apim.aeso.ca/
- **Polling at 60 seconds** — matches AESO's data refresh cadence exactly; faster polling returns duplicate data
- **MQTT uses a persistent connection** — one TCP socket for the entire process lifetime; reconnects automatically if the broker restarts
- **DNS retry** — transient DNS failures retry up to 3 times with 5 second delays before skipping that poll cycle; no restart needed
- **OpenSSL 3.x** — Azure APIM omits the TLS `close_notify` alert; the collector sets `SSL_OP_IGNORE_UNEXPECTED_EOF` via CFFI to handle this transparently
- **SMP vs Pool Price** — SMP is the per-minute marginal offer price; Pool Price is the hourly average. This collector records SMP.
