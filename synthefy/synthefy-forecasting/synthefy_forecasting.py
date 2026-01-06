"""
{
    "plugin_type": ["scheduled", "onwrite", "http"],
    "scheduled_args_config": [
        {
            "name": "measurement",
            "example": "temperature",
            "description": "InfluxDB measurement name to read from",
            "required": true
        },
        {
            "name": "field",
            "example": "value",
            "description": "Field name containing the time series values",
            "required": false
        },
        {
            "name": "tags",
            "example": "location=NYC,device=sensor1",
            "description": "Tag filters, comma-separated key=value pairs",
            "required": false
        },
        {
            "name": "time_range",
            "example": "30d",
            "description": "Time range to query historical data. Format: <number><unit> (e.g., '30d', '7d', '1h')",
            "required": false
        },
        {
            "name": "forecast_horizon",
            "example": "7d",
            "description": "Forecast horizon. Format: <number><unit> (e.g., '7d') or '<number> points' (e.g., '30 points')",
            "required": false
        },
        {
            "name": "model",
            "example": "prophet",
            "description": "Synthefy model to use (e.g., 'prophet', 'sfm-moe', 'autoarima')",
            "required": false
        },
        {
            "name": "api_key",
            "example": "your-api-key-here",
            "description": "Synthefy API key (required, or set SYNTHEFY_API_KEY environment variable)",
            "required": true
        },
        {
            "name": "api_url",
            "example": "https://api.synthefy.com",
            "description": "Synthefy API base URL",
            "required": false
        },
        {
            "name": "output_measurement",
            "example": "temperature_forecast",
            "description": "Output measurement name for forecast results (default: '{measurement}_forecast')",
            "required": false
        },
        {
            "name": "metadata_fields",
            "example": "humidity,pressure",
            "description": "Comma-separated list of metadata field names to use as covariates",
            "required": false
        }
    ],
    "onwrite_args_config": [
        {
            "name": "measurement",
            "example": "temperature",
            "description": "InfluxDB measurement name to read from",
            "required": true
        },
        {
            "name": "field",
            "example": "value",
            "description": "Field name containing the time series values",
            "required": false
        },
        {
            "name": "forecast_horizon",
            "example": "7d",
            "description": "Forecast horizon. Format: <number><unit> (e.g., '7d') or '<number> points'",
            "required": false
        },
        {
            "name": "model",
            "example": "prophet",
            "description": "Synthefy model to use",
            "required": false
        },
        {
            "name": "api_key",
            "example": "your-api-key-here",
            "description": "Synthefy API key (required, or set SYNTHEFY_API_KEY environment variable)",
            "required": true
        },
        {
            "name": "api_url",
            "example": "https://api.synthefy.com",
            "description": "Synthefy API base URL",
            "required": false
        }
    ],
    "http_args_config": [
        {
            "name": "measurement",
            "example": "temperature",
            "description": "InfluxDB measurement name to read from",
            "required": true
        },
        {
            "name": "field",
            "example": "value",
            "description": "Field name containing the time series values",
            "required": false
        },
        {
            "name": "forecast_horizon",
            "example": "7d",
            "description": "Forecast horizon. Format: <number><unit> (e.g., '7d') or '<number> points'",
            "required": false
        },
        {
            "name": "model",
            "example": "prophet",
            "description": "Synthefy model to use",
            "required": false
        },
        {
            "name": "api_key",
            "example": "your-api-key-here",
            "description": "Synthefy API key (required, or set SYNTHEFY_API_KEY environment variable)",
            "required": true
        },
        {
            "name": "api_url",
            "example": "https://api.synthefy.com",
            "description": "Synthefy API base URL",
            "required": false
        }
    ]
}
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

try:
    import pandas as pd
    import pyarrow as pa
    from influxdb_client_3 import InfluxDBClient3
except ImportError as e:
    raise ImportError(
        f"Required dependencies not installed: {e}. "
        "Please install: influxdb3-python pandas pyarrow"
    )

try:
    import httpx
except ImportError:
    try:
        import requests as httpx
    except ImportError:
        raise ImportError(
            "Either 'httpx' or 'requests' must be installed for API calls"
        )

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _parse_args(args: Dict[str, Any]) -> Dict[str, Any]:
    """Parse and validate plugin arguments."""
    parsed = {
        "measurement": args.get("measurement"),
        "field": args.get("field", "value"),
        "tags": args.get("tags", ""),
        "time_range": args.get("time_range", "30d"),
        "forecast_horizon": args.get("forecast_horizon", "7d"),
        "model": args.get("model", "prophet"),
        "api_key": args.get("api_key") or os.getenv("SYNTHEFY_API_KEY"),
        "api_url": args.get("api_url", "https://api.synthefy.com"),
        "output_measurement": args.get("output_measurement"),
        "metadata_fields": args.get("metadata_fields", ""),
    }

    if not parsed["measurement"]:
        raise ValueError("'measurement' argument is required")
    if not parsed["api_key"]:
        raise ValueError(
            "'api_key' argument is required or set SYNTHEFY_API_KEY environment variable"
        )

    if not parsed["output_measurement"]:
        parsed["output_measurement"] = f"{parsed['measurement']}_forecast"

    return parsed


def _parse_tags(tags_str: str) -> Dict[str, str]:
    """Parse tag filter string into dictionary."""
    if not tags_str:
        return {}
    tags = {}
    for pair in tags_str.split(","):
        if "=" in pair:
            key, value = pair.split("=", 1)
            tags[key.strip()] = value.strip()
    return tags


def _build_query(
    measurement: str,
    field: str,
    tags: Dict[str, str],
    time_range: str,
    metadata_fields: List[str],
) -> str:
    """Build SQL query to extract time series data from InfluxDB."""
    # Build SELECT clause
    select_fields = ["time", field]
    if metadata_fields:
        select_fields.extend(metadata_fields)

    # Add tag columns if they exist
    tag_columns = list(tags.keys())
    if tag_columns:
        select_fields.extend(tag_columns)

    select_clause = ", ".join(select_fields)

    # Build WHERE clause
    where_parts = [f"time >= now() - INTERVAL '{time_range}'"]

    if tags:
        tag_conditions = [f"{k} = '{v}'" for k, v in tags.items()]
        where_parts.extend(tag_conditions)

    where_clause = " AND ".join(where_parts)

    query = f"""
    SELECT {select_clause}
    FROM {measurement}
    WHERE {where_clause}
    ORDER BY time
    """

    return query


def _arrow_to_dataframe(arrow_table: pa.Table) -> pd.DataFrame:
    """Convert Arrow table to pandas DataFrame."""
    return arrow_table.to_pandas()


def _dataframe_to_synthefy_request(
    df: pd.DataFrame,
    field: str,
    forecast_horizon: str,
    metadata_fields: List[str],
    model: str,
) -> Dict[str, Any]:
    """Transform InfluxDB DataFrame to Synthefy ForecastV2Request format."""
    if df.empty:
        raise ValueError("No data found in query result")

    # Ensure time column is datetime
    if "time" not in df.columns:
        raise ValueError("Query result must include 'time' column")
    df["time"] = pd.to_datetime(df["time"])

    # Sort by time
    df = df.sort_values("time").reset_index(drop=True)

    # Extract history data
    history_timestamps = df["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ").tolist()
    # Convert NaN to None - pandas fillna doesn't accept None as value in newer versions
    history_values = [None if pd.isna(val) else val for val in df[field].tolist()]

    # Calculate forecast horizon
    if forecast_horizon.endswith("d"):
        days = int(forecast_horizon[:-1])
        forecast_timedelta = timedelta(days=days)
    elif forecast_horizon.endswith("h"):
        hours = int(forecast_horizon[:-1])
        forecast_timedelta = timedelta(hours=hours)
    elif forecast_horizon.endswith(" points"):
        num_points = int(forecast_horizon.replace(" points", ""))
        # Estimate time delta from last two points
        if len(df) >= 2:
            time_delta = df["time"].iloc[-1] - df["time"].iloc[-2]
            forecast_timedelta = time_delta * num_points
        else:
            forecast_timedelta = timedelta(days=7)  # Default
    else:
        # Default to 7 days
        forecast_timedelta = timedelta(days=7)

    # Generate target timestamps
    last_timestamp = df["time"].iloc[-1]
    target_timestamps = []

    # Calculate time step from data
    if len(df) >= 2:
        time_step = df["time"].iloc[-1] - df["time"].iloc[-2]
        num_points = max(1, int(forecast_timedelta / time_step))
    else:
        # Default to hourly if only one point
        time_step = timedelta(hours=1)
        num_points = max(1, int(forecast_timedelta / time_step))

    current_time = last_timestamp + time_step
    for _ in range(num_points):
        target_timestamps.append(current_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        current_time += time_step

    target_values = [None] * len(target_timestamps)

    # Build samples
    samples = []

    # Main forecast sample
    forecast_sample = {
        "sample_id": field,
        "history_timestamps": history_timestamps,
        "history_values": history_values,
        "target_timestamps": target_timestamps,
        "target_values": target_values,
        "forecast": True,
        "metadata": False,
        "leak_target": False,
        "column_name": field,
    }

    # Metadata samples
    metadata_samples = []
    for metadata_field in metadata_fields:
        if metadata_field in df.columns:
            metadata_sample = {
                "sample_id": metadata_field,
                "history_timestamps": history_timestamps,
                # Convert NaN to None - pandas fillna doesn't accept None as value in newer versions
                "history_values": [
                    None if pd.isna(val) else val for val in df[metadata_field].tolist()
                ],
                "target_timestamps": target_timestamps,
                "target_values": [None] * len(target_timestamps),
                "forecast": False,
                "metadata": True,
                "leak_target": False,
                "column_name": metadata_field,
            }
            metadata_samples.append(metadata_sample)

    # Combine all samples
    sample_row = [forecast_sample] + metadata_samples
    samples.append(sample_row)

    request = {"samples": samples, "model": model}

    return request


def _call_synthefy_api(
    request_data: Dict[str, Any], api_url: str, api_key: str
) -> Dict[str, Any]:
    """Call Synthefy Forecasting API."""
    endpoint = f"{api_url.rstrip('/')}/v2/forecast"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": api_key,
    }

    logger.info(f"Calling Synthefy API: {endpoint}")
    logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")

    try:
        if hasattr(httpx, "post"):
            # Using requests library
            response = httpx.post(
                endpoint, json=request_data, headers=headers, timeout=300
            )
        else:
            # Using httpx library
            response = httpx.post(
                endpoint, json=request_data, headers=headers, timeout=300.0
            )

        response.raise_for_status()
        result = response.json()
        logger.info("Synthefy API call successful")
        return result
    except Exception as e:
        logger.error(f"Synthefy API call failed: {e}")
        raise


def _forecast_response_to_line_protocol(
    forecast_response: Dict[str, Any],
    output_measurement: str,
    tags: Dict[str, str],
    model: str,
    field_name: Optional[str] = None,
) -> str:
    """Transform Synthefy forecast response to InfluxDB Line Protocol format."""
    lines = []

    if "forecasts" not in forecast_response:
        raise ValueError("Invalid forecast response: missing 'forecasts' field")

    forecasts = forecast_response["forecasts"]
    if not forecasts or not forecasts[0]:
        raise ValueError("No forecasts in response")

    # Get the first forecast (we support single time series for now)
    forecast_row = forecasts[0]

    # The API returns SingleSampleForecastPayload objects as dicts in JSON
    # Find the forecast sample (not metadata)
    forecast_payload = None
    for f in forecast_row:
        # API response is JSON, so f will be a dict
        if isinstance(f, dict):
            # SingleSampleForecastPayload doesn't have "forecast" field
            # It's identified by having timestamps and values
            if "timestamps" in f and "values" in f:
                forecast_payload = f
                break
        else:
            # Handle object-like structures (shouldn't happen with JSON API)
            if hasattr(f, "timestamps") and hasattr(f, "values"):
                forecast_payload = {
                    "sample_id": getattr(f, "sample_id", "value"),
                    "timestamps": getattr(f, "timestamps", []),
                    "values": getattr(f, "values", []),
                    "quantiles": getattr(f, "quantiles", None),
                }
                break

    if not forecast_payload:
        # Fallback: use first item if available
        if forecast_row:
            first_item = forecast_row[0]
            if isinstance(first_item, dict):
                forecast_payload = first_item
            else:
                forecast_payload = {
                    "sample_id": getattr(first_item, "sample_id", "value"),
                    "timestamps": getattr(first_item, "timestamps", []),
                    "values": getattr(first_item, "values", []),
                    "quantiles": getattr(first_item, "quantiles", None),
                }
        else:
            raise ValueError("No forecast payload found in response")

    timestamps = forecast_payload.get("timestamps", [])
    values = forecast_payload.get("values", [])
    quantiles = forecast_payload.get("quantiles", {})

    # Build tags string
    tag_parts = list(tags.items())
    tag_parts.append(("model", model))
    tags_str = ",".join([f"{k}={v}" for k, v in tag_parts])

    # Convert timestamps to nanoseconds
    for i, (ts_str, value) in enumerate(zip(timestamps, values)):
        if value is None:
            continue

        # Parse timestamp
        try:
            ts = pd.to_datetime(ts_str)
            ts_ns = int(ts.timestamp() * 1e9)
        except Exception:
            logger.warning(f"Could not parse timestamp: {ts_str}")
            continue

        # Build fields - use provided field_name or sample_id from payload
        output_field_name = field_name or forecast_payload.get("sample_id", "value")
        # Escape field name if it contains special characters (Line Protocol requirement)
        if " " in output_field_name or "," in output_field_name:
            output_field_name = f'"{output_field_name}"'
        fields = [f"{output_field_name}={value}"]

        # Add quantiles if available
        if quantiles:
            for q_level, q_values in quantiles.items():
                if i < len(q_values) and q_values[i] is not None:
                    fields.append(f"value_{q_level}={q_values[i]}")

        fields_str = ",".join(fields)

        # Build line protocol
        line = f"{output_measurement},{tags_str} {fields_str} {ts_ns}"
        lines.append(line)

    return "\n".join(lines)


def _get_influxdb_client(
    influxdb3_local: Any, database: str, token: Optional[str] = None
) -> InfluxDBClient3:
    """Get or create InfluxDB client connection."""
    # Try to get client from influxdb3_local
    if hasattr(influxdb3_local, "_client") and influxdb3_local._client:
        return influxdb3_local._client

    # Try to get connection info from influxdb3_local
    host = getattr(influxdb3_local, "host", None) or os.getenv(
        "INFLUXDB_HOST", "http://localhost:8181"
    )

    # Create new client
    client = InfluxDBClient3(
        host=host,
        database=database,
        token=token,
    )
    return client


def _write_forecasts_to_influxdb(
    influxdb3_local: Any,
    line_protocol_data: str,
    database: str,
    token: Optional[str] = None,
) -> None:
    """Write forecast data to InfluxDB using Line Protocol."""
    forecast_points = len(
        [line for line in line_protocol_data.split("\n") if line.strip()]
    )
    logger.info(f"Writing {forecast_points} forecast points to InfluxDB")

    try:
        # Get InfluxDB client
        client = _get_influxdb_client(influxdb3_local, database, token)

        # Write using Line Protocol
        client.write(record=line_protocol_data, database=database)

        logger.info("Forecasts written successfully to InfluxDB")
    except Exception as e:
        logger.error(f"Failed to write forecasts to InfluxDB: {e}")
        raise


def process_scheduled_call(
    influxdb3_local: Any, call_time: datetime, args: Dict[str, Any]
) -> None:
    """
    Process scheduled forecasting call.

    This function is called by InfluxDB 3 when a scheduled trigger fires.

    Args:
        influxdb3_local: InfluxDB 3 local client instance
        call_time: Time when the plugin was triggered
        args: Plugin configuration arguments
    """
    logger.info(f"Starting scheduled forecast at {call_time}")

    try:
        # Parse arguments
        parsed_args = _parse_args(args)
        logger.info(
            f"Plugin arguments: measurement={parsed_args['measurement']}, model={parsed_args['model']}"
        )

        # Get database name from influxdb3_local
        database = getattr(influxdb3_local, "database", None) or args.get(
            "database", "mydb"
        )

        # Parse tags and metadata fields
        tags = _parse_tags(parsed_args["tags"])
        metadata_fields = [
            f.strip() for f in parsed_args["metadata_fields"].split(",") if f.strip()
        ]

        # Build and execute query
        query = _build_query(
            parsed_args["measurement"],
            parsed_args["field"],
            tags,
            parsed_args["time_range"],
            metadata_fields,
        )
        logger.info(f"Executing query: {query}")

        # Query InfluxDB
        client = _get_influxdb_client(influxdb3_local, database)
        result = client.query(query=query, language="sql")

        # Convert Arrow table to DataFrame
        df = _arrow_to_dataframe(result)
        logger.info(f"Retrieved {len(df)} data points")

        if df.empty:
            logger.warning("No data found, skipping forecast")
            return

        # Transform to Synthefy API format
        synthefy_request = _dataframe_to_synthefy_request(
            df,
            parsed_args["field"],
            parsed_args["forecast_horizon"],
            metadata_fields,
            parsed_args["model"],
        )

        # Call Synthefy API
        forecast_response = _call_synthefy_api(
            synthefy_request,
            parsed_args["api_url"],
            parsed_args["api_key"],
        )

        # Transform response to Line Protocol
        line_protocol = _forecast_response_to_line_protocol(
            forecast_response,
            parsed_args["output_measurement"],
            tags,
            parsed_args["model"],
            parsed_args["field"],
        )

        # Write back to InfluxDB
        # Get token from args or environment
        token = args.get("influxdb_token") or os.getenv("INFLUXDB_TOKEN")
        _write_forecasts_to_influxdb(influxdb3_local, line_protocol, database, token)

        logger.info("Scheduled forecast completed successfully")

    except Exception as e:
        logger.error(f"Scheduled forecast failed: {e}", exc_info=True)
        raise


def process_writes(influxdb3_local: Any, args: Dict[str, Any]) -> None:
    """
    Process on-write forecasting trigger.

    This function is called by InfluxDB 3 when new data is written to a table.

    Args:
        influxdb3_local: InfluxDB 3 local client instance
        args: Plugin configuration arguments
    """
    logger.info("Starting on-write forecast")

    try:
        # Parse arguments
        parsed_args = _parse_args(args)
        database = getattr(influxdb3_local, "database", None) or args.get(
            "database", "mydb"
        )

        # For on-write, we query recent data (last hour or so)
        # This is a simplified implementation
        tags = _parse_tags(parsed_args.get("tags", ""))
        metadata_fields = [
            f.strip()
            for f in parsed_args.get("metadata_fields", "").split(",")
            if f.strip()
        ]

        # Query recent data
        query = _build_query(
            parsed_args["measurement"],
            parsed_args["field"],
            tags,
            "1h",  # Last hour for on-write
            metadata_fields,
        )

        client = _get_influxdb_client(influxdb3_local, database)
        result = client.query(query=query, language="sql")

        df = _arrow_to_dataframe(result)

        if df.empty:
            logger.warning("No recent data found, skipping forecast")
            return

        # Transform and call API (same as scheduled)
        synthefy_request = _dataframe_to_synthefy_request(
            df,
            parsed_args["field"],
            parsed_args["forecast_horizon"],
            metadata_fields,
            parsed_args["model"],
        )

        forecast_response = _call_synthefy_api(
            synthefy_request,
            parsed_args["api_url"],
            parsed_args["api_key"],
        )

        line_protocol = _forecast_response_to_line_protocol(
            forecast_response,
            parsed_args["output_measurement"],
            tags,
            parsed_args["model"],
        )

        token = args.get("influxdb_token") or os.getenv("INFLUXDB_TOKEN")
        _write_forecasts_to_influxdb(influxdb3_local, line_protocol, database, token)

        logger.info("On-write forecast completed successfully")

    except Exception as e:
        logger.error(f"On-write forecast failed: {e}", exc_info=True)
        raise


def process_http_request(
    influxdb3_local: Any, request_body: Dict[str, Any], args: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process HTTP request forecasting trigger.

    This function is called by InfluxDB 3 when an HTTP request is made to the plugin.

    Args:
        influxdb3_local: InfluxDB 3 local client instance
        request_body: HTTP request body
        args: Plugin configuration arguments
    """
    logger.info("Starting HTTP request forecast")

    try:
        # Merge request body with args (request body can override args)
        merged_args = {**args, **request_body}
        parsed_args = _parse_args(merged_args)
        database = getattr(influxdb3_local, "database", None) or args.get(
            "database", "mydb"
        )

        tags = _parse_tags(parsed_args.get("tags", ""))
        metadata_fields = [
            f.strip()
            for f in parsed_args.get("metadata_fields", "").split(",")
            if f.strip()
        ]

        # Query data
        query = _build_query(
            parsed_args["measurement"],
            parsed_args["field"],
            tags,
            parsed_args["time_range"],
            metadata_fields,
        )

        client = _get_influxdb_client(influxdb3_local, database)
        result = client.query(query=query, language="sql")

        df = _arrow_to_dataframe(result)

        if df.empty:
            return {"error": "No data found", "status": "error"}

        # Transform and call API
        synthefy_request = _dataframe_to_synthefy_request(
            df,
            parsed_args["field"],
            parsed_args["forecast_horizon"],
            metadata_fields,
            parsed_args["model"],
        )

        forecast_response = _call_synthefy_api(
            synthefy_request,
            parsed_args["api_url"],
            parsed_args["api_key"],
        )

        line_protocol = _forecast_response_to_line_protocol(
            forecast_response,
            parsed_args["output_measurement"],
            tags,
            parsed_args["model"],
        )

        token = args.get("influxdb_token") or os.getenv("INFLUXDB_TOKEN")
        _write_forecasts_to_influxdb(influxdb3_local, line_protocol, database, token)

        return {
            "status": "success",
            "message": "Forecast generated and written to InfluxDB",
            "forecast_points": len(line_protocol.split("\n")),
        }

    except Exception as e:
        logger.error(f"HTTP request forecast failed: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}
