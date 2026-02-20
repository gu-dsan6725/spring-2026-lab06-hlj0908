"""
Week 4 Lab: World Bank Data MCP Server

An MCP server that exposes:
- Resources: Local World Bank indicator data from CSV
- Tools: Live data from REST Countries and World Bank APIs

Transport: Streamable HTTP on port 8765
"""

import json
import logging
from pathlib import Path
from typing import Optional

import httpx
import polars as pl
from mcp.server.fastmcp import FastMCP


# =============================================================================
# CONFIGURATION
# =============================================================================

DATA_FILE: Path = Path(__file__).parent / "data" / "world_bank_indicators.csv"
HOST: str = "127.0.0.1"
PORT: int = 8765

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)

mcp = FastMCP(
    "world-bank-server",
    host=HOST,
    port=PORT,
)


# =============================================================================
# PRIVATE HELPER FUNCTIONS
# =============================================================================

def _load_data() -> pl.DataFrame:
    """Load the World Bank indicators CSV file."""
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")
    return pl.read_csv(DATA_FILE)


def _fetch_rest_countries(country_code: str) -> dict:
    """Fetch country info from REST Countries API."""
    url = f"https://restcountries.com/v3.1/alpha/{country_code}"
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()[0]


def _fetch_world_bank_indicator(
    country_code: str,
    indicator: str,
    year: Optional[int] = None,
) -> list:
    """Fetch indicator from World Bank API."""
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator}"
    params = {"format": "json", "per_page": 100}
    if year:
        params["date"] = str(year)

    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if len(data) < 2 or not data[1]:
            return []
        return data[1]


# =============================================================================
# PART 1: RESOURCES (Local Data)
# =============================================================================

@mcp.resource("data://schema")
def get_schema() -> str:
    """Return the schema of the World Bank dataset."""
    df = _load_data()
    schema_info = {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
    return json.dumps(schema_info, indent=2)


@mcp.resource("data://countries")
def get_countries() -> str:
    """List all unique countries in the dataset."""
    df = _load_data()

    countries_df = (
        df
        .select(["countryiso3code", "country"])
        .unique()
        .sort("countryiso3code")
    )

    return countries_df.write_json()


@mcp.resource("data://indicators/{country_code}")
def get_country_indicators(country_code: str) -> str:
    """Get all indicators for a specific country from local data."""
    df = _load_data()

    filtered_df = df.filter(
        pl.col("countryiso3code") == country_code.upper()
    )

    if filtered_df.is_empty():
        return json.dumps(
            {"error": f"No data found for country code: {country_code}"},
            indent=2
        )

    return filtered_df.write_json()


# =============================================================================
# PART 2: TOOLS (External APIs)
# =============================================================================

@mcp.tool()
def get_country_info(country_code: str) -> dict:
    """Fetch detailed information about a country from REST Countries API."""
    logger.info(f"Fetching country info for: {country_code}")

    try:
        data = _fetch_rest_countries(country_code)
    except httpx.HTTPStatusError:
        logger.error(f"Invalid country code: {country_code}")
        return {"error": f"Country not found: {country_code}"}
    except Exception as e:
        logger.error(f"REST Countries API failure: {e}")
        return {"error": "Failed to fetch country information"}

    return {
        "name": data.get("name", {}).get("common"),
        "capital": data.get("capital", [None])[0],
        "region": data.get("region"),
        "subregion": data.get("subregion"),
        "languages": list(data.get("languages", {}).values()),
        "currencies": list(data.get("currencies", {}).keys()),
        "population": data.get("population"),
        "flag": data.get("flag"),
    }


@mcp.tool()
def get_live_indicator(
    country_code: str,
    indicator: str,
    year: int = 2022,
) -> dict:
    """Fetch a specific indicator value from the World Bank API."""
    logger.info(f"Fetching {indicator} for {country_code} in {year}")

    try:
        results = _fetch_world_bank_indicator(country_code, indicator, year)
    except Exception as e:
        logger.error(f"World Bank API failure: {e}")
        return {"error": "World Bank API request failed"}

    if not results:
        return {
            "country": country_code,
            "indicator": indicator,
            "year": year,
            "value": None,
        }

    record = results[0]

    return {
        "country": record.get("country", {}).get("id"),
        "country_name": record.get("country", {}).get("value"),
        "indicator": record.get("indicator", {}).get("id"),
        "indicator_name": record.get("indicator", {}).get("value"),
        "year": int(record.get("date")),
        "value": record.get("value"),
    }


@mcp.tool()
def compare_countries(
    country_codes: list[str],
    indicator: str,
    year: int = 2022,
) -> list[dict]:
    """Compare an indicator across multiple countries."""
    logger.info(f"Comparing {indicator} for countries: {country_codes}")

    results = []

    for code in country_codes:
        try:
            result = get_live_indicator(code, indicator, year)
            results.append(result)
        except Exception as e:
            logger.error(f"Comparison failed for {code}: {e}")
            results.append({
                "country": code,
                "indicator": indicator,
                "year": year,
                "value": None,
            })

    return results

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    logger.info(f"Starting World Bank MCP Server on http://{HOST}:{PORT}/mcp")
    logger.info(f"Connect with MCP Inspector or test client at http://{HOST}:{PORT}/mcp")
    logger.info("Press Ctrl+C to stop")
    mcp.run(transport="streamable-http")
