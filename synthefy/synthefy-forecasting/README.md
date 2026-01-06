# Synthefy Forecasting Plugin

⚡ scheduled, onwrite, http  
🏷️ forecasting, time-series, predictive-analytics  
🔧 InfluxDB 3 Core, InfluxDB 3 Enterprise

## Description

The Synthefy Forecasting Plugin integrates Synthefy Forecasting API with InfluxDB 3 to enable automated time series forecasting workflows. It reads time series data from InfluxDB, generates forecasts using Synthefy's advanced forecasting models and writes the results back to InfluxDB for visualization and alerting.

**Key Features:**

- **Automated Forecasting**: Generate forecasts on a schedule, when new data arrives, or on-demand via HTTP
- **Multiple Models**: Support for various Synthefy models
- **Metadata Support**: Use additional fields as covariates for improved forecasting accuracy
- **Tag Filtering**: Filter data by tags (e.g., location, device) for targeted forecasting
- **Line Protocol Writes**: Reliable data writing using InfluxDB Line Protocol

## Configuration

Plugin parameters may be specified as key-value pairs in the `--trigger-arguments` flag (CLI) or in the `trigger_arguments` field (API) when creating a trigger.

### Plugin metadata

This plugin includes a JSON metadata schema in its docstring that defines supported trigger types and configuration parameters. This metadata enables the [InfluxDB 3 Explorer](https://docs.influxdata.com/influxdb3/explorer/) UI to display and configure the plugin.

### Scheduled trigger parameters

| Parameter            | Type   | Default  | Description                                                       |
|----------------------|--------|----------|-------------------------------------------------------------------|
| `measurement`        | string | required | Source measurement containing historical data                     |
| `field`              | string | "value"  | Field name to forecast                                            |
| `tags`               | string | ""       | Tag filters, comma-separated (e.g., "location=NYC,device=sensor1") |
| `time_range`         | string | "30d"    | Historical data window. Format: `<number><unit>` (e.g., "30d")    |
| `forecast_horizon`   | string | "7d"     | Forecast duration. Format: `<number><unit>` or "<number> points"  |
| `model`              | string | "prophet"| Synthefy model to use (e.g., "prophet", "sfm-moe", "autoarima")   |
| `api_key`            | string | required | Synthefy API key (or set SYNTHEFY_API_KEY environment variable)   |
| `api_url`            | string | "<https://forecast.synthefy.com>" | Synthefy API base URL                              |
| `output_measurement` | string | "{measurement}_forecast" | Destination measurement for forecast results          |
| `metadata_fields`    | string | ""       | Comma-separated list of metadata field names to use as covariates |

### On-write trigger parameters

| Parameter            | Type   | Default  | Description                                                       |
|----------------------|--------|----------|-------------------------------------------------------------------|
| `measurement`        | string | required | Source measurement containing historical data                     |
| `field`              | string | "value"  | Field name to forecast                                            |
| `forecast_horizon`   | string | "7d"     | Forecast duration. Format: `<number><unit>` or "<number> points"  |
| `model`              | string | "prophet"| Synthefy model to use                                             |
| `api_key`            | string | required | Synthefy API key (or set SYNTHEFY_API_KEY environment variable)   |
| `api_url`            | string | "<https://forecast.synthefy.com>" | Synthefy API base URL                              |

### HTTP trigger parameters

| Parameter            | Type   | Default  | Description                                                       |
|----------------------|--------|----------|-------------------------------------------------------------------|
| `measurement`        | string | required | Source measurement containing historical data                     |
| `field`              | string | "value"  | Field name to forecast                                            |
| `forecast_horizon`   | string | "7d"     | Forecast duration. Format: `<number><unit>` or "<number> points"  |
| `model`              | string | "prophet"| Synthefy model to use                                             |
| `api_key`            | string | required | Synthefy API key (or set SYNTHEFY_API_KEY environment variable)   |
| `api_url`            | string | "<https://forecast.synthefy.com>" | Synthefy API base URL                              |

## Requirements

### Dependencies

- Python 3.7 or higher
- `influxdb3-python` - InfluxDB 3 Python client
- `pandas` - Data manipulation
- `pyarrow` - Arrow table support
- `httpx` or `requests` - HTTP client for API calls

### Installation

```bash
pip install influxdb3-python pandas pyarrow httpx
```

### Prerequisites

- InfluxDB 3 Core or Enterprise installed and running
- Synthefy API key (obtain from [Synthefy](https://synthefy.com))

## Trigger Setup

### Scheduled trigger

Generate forecasts on a schedule (e.g., every hour):

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename synthefy/synthefy_forecasting/synthefy_forecasting.py \
  --trigger-spec "every:1h" \
  --trigger-arguments measurement=temperature,field=value,api_key=YOUR_API_KEY,model=prophet,forecast_horizon=7d \
  temperature_forecast_trigger
```

### On-write trigger

Generate forecasts automatically when new data is written:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename synthefy/synthefy_forecasting/synthefy_forecasting.py \
  --trigger-spec "on_write" \
  --trigger-arguments measurement=temperature,field=value,api_key=YOUR_API_KEY \
  temperature_forecast_onwrite
```

### HTTP trigger

Generate forecasts on-demand via HTTP request:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename synthefy/synthefy_forecasting/synthefy_forecasting.py \
  --trigger-spec "request:forecast" \
  --trigger-arguments measurement=temperature,field=value,api_key=YOUR_API_KEY \
  temperature_forecast_http
```

Then call via HTTP:

```bash
curl -X POST http://localhost:8181/api/v3/plugins/forecast \
  -H "Content-Type: application/json" \
  -d '{"measurement": "temperature", "model": "prophet"}'
```

## Example Usage

### Basic forecast

Forecast temperature data with default settings:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename synthefy/synthefy_forecasting/synthefy_forecasting.py \
  --trigger-spec "every:1d" \
  --trigger-arguments measurement=temperature,field=value,api_key=YOUR_API_KEY \
  daily_temperature_forecast
```

### Forecast with tag filtering

Forecast only for specific location:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename synthefy/synthefy_forecasting/synthefy_forecasting.py \
  --trigger-spec "every:1h" \
  --trigger-arguments measurement=temperature,field=value,tags=location=NYC,api_key=YOUR_API_KEY \
  nyc_temperature_forecast
```

### Forecast with metadata

Use humidity as a covariate for improved accuracy:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename synthefy/synthefy_forecasting/synthefy_forecasting.py \
  --trigger-spec "every:1h" \
  --trigger-arguments measurement=temperature,field=value,metadata_fields=humidity,api_key=YOUR_API_KEY \
  temperature_with_humidity_forecast
```

### Advanced model

Use SFM-MOE model for more accurate forecasts:

```bash
influxdb3 create trigger \
  --database mydb \
  --plugin-filename synthefy/synthefy_forecasting/synthefy_forecasting.py \
  --trigger-spec "every:6h" \
  --trigger-arguments measurement=sales,field=revenue,model=sfm-moe,forecast_horizon=30d,api_key=YOUR_API_KEY \
  sales_forecast_advanced
```

## Output Format

Forecasts are written to a new measurement (default: `{measurement}_forecast`) with the following structure:

- **Measurement**: `{measurement}_forecast` (configurable via `output_measurement`)
- **Tags**: Original tags + `model={model_name}`
- **Fields**:
  - `{field_name}`: Forecasted values
  - `value_{quantile}`: Quantile forecasts if available (e.g., `value_0.1`, `value_0.9`)

Example Line Protocol output:

```
temperature_forecast,location=NYC,model=prophet value=72.5,value_0.1=71.2,value_0.9=73.8 1704672000000000000
```

## Querying Forecasts

Query forecast results:

```sql
SELECT * FROM temperature_forecast 
WHERE time >= now() - INTERVAL '7 days'
ORDER BY time
```

Compare historical and forecasted data:

```sql
SELECT time, value as actual 
FROM temperature 
WHERE time >= now() - INTERVAL '30 days'

UNION ALL

SELECT time, value as forecast 
FROM temperature_forecast 
WHERE time >= now() - INTERVAL '7 days'
ORDER BY time
```

## Supported Models

- `prophet`: Facebook Prophet (statistical model)
- `sfm-moe`: Synthefy Foundation Model (advanced ML model)
- `autoarima`: Auto ARIMA (statistical model)
- And more - see [Synthefy API documentation](https://docs.synthefy.com) for full list

## Troubleshooting

### No data found

If you see "No data found" errors:

- Check that the measurement exists
- Verify the time range includes data
- Check tag filters match your data

### API errors

If Synthefy API calls fail:

- Verify your API key is correct
- Check API URL is accessible
- Review API rate limits
- Check network connectivity

### Write errors

If writes fail:

- Ensure database exists
- Check plugin has write permissions
- Verify Line Protocol format is correct
- Check InfluxDB connection

## Limitations

- Currently supports single time series per trigger execution
- Forecast horizon calculation assumes regular time intervals
- Large time series (>10K points) may need sampling (not yet implemented)

## License

MIT or Apache 2.0 (user's choice)

## Support

For issues and questions:

- Plugin issues: Open an issue in the [influxdb3_plugins repository](https://github.com/influxdata/influxdb3_plugins)
- Synthefy API: Contact [Synthefy support](https://synthefy.com) or see [Synthefy documentation](https://docs.synthefy.com)
- InfluxDB: See [InfluxDB documentation](https://docs.influxdata.com/influxdb3/)
