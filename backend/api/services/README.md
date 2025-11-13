# Weather API Technical Specifications

## Introduction

This document describes the technical specifications of the six public weather APIs integrated into the system. The information was validated in November 2025 and represents the current state of the services.

## Weather APIs

### MET Norway Locationforecast API

- **Documentation**: https://api.met.no/weatherapi/locationforecast/2.0/documentation
- **Data Model**: https://docs.api.met.no/doc/locationforecast/datamodel
- **Coverage**: Global
- **Resolution**: Hourly (aggregated to daily)
- **Period**: 5-day forecast
- **License**: CC-BY 4.0 (attribution required in all visualizations)

#### Extracted Variables

**Instant Values (hourly snapshots):**
- `air_temperature`: Air temperature (°C)
- `relative_humidity`: Relative humidity (%)
- `wind_speed`: Wind speed at 10m (m/s)

**Next 1 hour:**
- `precipitation_amount`: Hourly precipitation (mm)

**Next 6 hours:**
- `air_temperature_max`: Maximum temperature (°C)
- `air_temperature_min`: Minimum temperature (°C)
- `precipitation_amount`: 6-hour precipitation (mm)

#### Request Limits

| Period | Limit |
|--------|-------|
| **Per Second** | Maximum 20 requests/second per application (total, including all users). Above this requires special agreement; excess requests are throttled (HTTP 429 code). |
| **Per Minute** | Recommended <100 req/min (fair use); excess requests are blocked. |
| **Per Hour/Day/Month** | Not strictly specified; fair use emphasizes cache and avoiding repetitions. Monitor "Expires" headers to avoid repeating before indicated time. |
| **General Policy** | Mandatory identification via User-Agent (e.g., "MyApp/1.0 (your@email.com)"). Violation leads to permanent ban. HTTPS mandatory. |

### NASA POWER API

- **Website**: https://power.larc.nasa.gov/
- **Documentation**: https://power.larc.nasa.gov/docs/services/api/
- **Citation**: https://power.larc.nasa.gov/docs/referencing/
- **License**: Free use (Public Domain)
- **Version**: Daily 2.x.x
- **Community**: `AG` (Agronomy) - mandatory for agroclimatic data

#### Mandatory Attribution
"Data obtained from NASA Langley Research Center POWER Project funded through the NASA Earth Science Directorate Applied Science Program."

#### Variables and Spatial Resolution

| Variable | Description | Spatial Resolution |
|----------|-------------|-------------------|
| `ALLSKY_SFC_SW_DWN` | CERES SYN1deg All Sky Surface Shortwave Downward Irradiance (MJ/m²/day) | 1° × 1° |
| `T2M` | MERRA-2 Temperature at 2 Meters (°C) | 0.5° × 0.625° |
| `T2M_MAX` | MERRA-2 Temperature at 2 Meters Maximum (°C) | 0.5° × 0.625° |
| `T2M_MIN` | MERRA-2 Temperature at 2 Meters Minimum (°C) | 0.5° × 0.625° |
| `RH2M` | MERRA-2 Relative Humidity at 2 Meters (%) | 0.5° × 0.625° |
| `WS2M` | MERRA-2 Wind Speed at 2 Meters (m/s) | 0.5° × 0.625° |
| `PRECTOTCORR` | MERRA-2 Precipitation Corrected (mm/day) | 0.5° × 0.625° |

#### Request Limits

| Period | Limit |
|--------|-------|
| **Per Second/Minute/Hour** | Not strictly specified; fair use recommended (avoid >1 req/s to avoid overloading). Typical response time: 1-5 seconds. |
| **Per Day/Month** | No numerical limits; limit of 20 parameters per request (for single point). For regions, 1 parameter per request. Avoid resolutions <0.5° (unnecessary repetition). |
| **General Policy** | Monitor HTTP codes for failures. Intensive requests (e.g., global grid) should be optimized; notify excessive use. |

### NWS Forecast API (NOAA)

- **Documentation**: https://www.weather.gov/documentation/services-web-api
- **FAQ**: https://weather-gov.github.io/api/general-faqs
- **License**: Public Domain (US Government)
- **Coverage**: Continental USA (bbox: -125°W to -66°W, 24°N to 49°N)
- **Extension**: Includes Alaska, Hawaii (lat 18°N to 71°N)
- **Resolution**: Hourly → Daily
- **Period**: 5-day forecast (120 hours)

#### Technical Characteristics
- **User-Agent**: Mandatory according to documentation
- **Rate Limit**: ≈5 requests/second
- **Automatic Conversion**: °F → °C, mph → m/s
- **Aggregation**: Mean (temp/humidity/wind), Sum (precip), Max/Min (temp)

#### Request Limits

| Period | Limit |
|--------|-------|
| **Per Second** | ≈5 req/s (maximum; excess returns error 429, retry after 5s). |
| **Per Minute/Hour** | Not specified; fair use with intervals >1 min for retries. |
| **Per Day/Month** | No strict limits; monitored for abuse (e.g., >1000 req/day may trigger review). |
| **General Policy** | User-Agent mandatory (e.g., "MyApp/1.0 (your@email.com)"). HTTPS; no authentication. |

#### Known Issues (2025)
- API may return past data (automatically filtered)
- Minimum temperature has greater variation (nocturnal microclimate)
- Precipitation: uses `quantitativePrecipitation` when available

### NWS Stations API (NOAA)

- **Documentation**: Same as NWS Forecast API
- **License**: Public Domain
- **Coverage**: ≈1800 stations in USA
- **EVAonline default period**: 01/01/1990 – present
- **Resolution**: Hourly → Daily

#### Typical Workflow
1. `find_nearest_stations(lat, lon)` → Nearest stations ordered
2. `get_station_observations(station_id, start, end)` → Observations
3. Aggregate to daily: mean (temp/humidity/wind), sum (precip)

#### Request Limits

| Period | Limit |
|--------|-------|
| **Per Second** | ≈5 req/s (maximum; excess returns error 429, retry after 5s). |
| **Per Minute/Hour** | Not specified; fair use with intervals >1 min for retries. |
| **Per Day/Month** | No strict limits; monitored for abuse (e.g., >1000 req/day may trigger review). |
| **General Policy** | User-Agent mandatory (e.g., "MyApp/1.0 (your@email.com)"). HTTPS; no authentication. |

#### Known Issues (2025)
- Observations may have up to 20-minute delay (MADIS)
- Null values in max/min temperature outside CST
- Precipitation <0.4" may be reported as 0 (rounding)

### Open-Meteo Archive API

- **Endpoint**: https://archive-api.open-meteo.com/v1/archive
- **Documentation**: https://open-meteo.com/en/docs
- **Source Code**: https://github.com/open-meteo/open-meteo (AGPLv3)
- **License**: CC BY 4.0 + AGPLv3
- **Coverage**: Global
- **Resolution**: Daily
- **EVAonline default period**: 01/01/1990 to (today - 2 days)

#### Available Variables (10)
- **Temperature**: `temperature_2m_max/mean/min` (°C)
- **Relative Humidity**: `relative_humidity_2m_max/mean/min` (%)
- **Wind**: `wind_speed_10m_mean` (m/s)
- **Radiation**: `shortwave_radiation_sum` (MJ/m²)
- **Precipitation**: `precipitation_sum` (mm)
- **ETo**: `et0_fao_evapotranspiration` (mm)

#### Request Limits

| Period | Limit |
|--------|-------|
| **Per Second/Minute/Hour** | Fair use; recommended <10 req/s to avoid throttling. |
| **Per Day** | ≈10,000 req/day in free plan (non-commercial); excess may be limited. |
| **Per Month** | Not strict; paid plans for >1M req/month. |
| **General Policy** | API key optional for tracking; cache mandatory for repetitions. |

#### Cache Strategy (Nov 2025)
- **Primary**: Redis via ClimateCache (recommended)
- **Fallback**: `requests_cache` local
- **TTL**: 24h (historical data is stable)

### Open-Meteo Forecast API

- **Endpoint**: https://api.open-meteo.com/v1/forecast
- **Documentation**: https://open-meteo.com/en/docs
- **Source Code**: https://github.com/open-meteo/open-meteo (AGPLv3)
- **License**: CC BY 4.0 + AGPLv3
- **Coverage**: Global
- **Resolution**: Daily
- **Period**: (today - 25 days) to (today + 5 days) = 30 days total

#### Variables
Identical to Open-Meteo Archive

#### Request Limits

| Period | Limit |
|--------|-------|
| **Per Second/Minute/Hour** | Fair use; recommended ≈10 req/s to avoid throttling. |
| **Per Day** | ≈10,000 req/day in free plan (non-commercial); excess may be limited. |
| **Per Month** | Not strict; paid plans for >1M req/month. |
| **General Policy** | API key optional for tracking; cache mandatory for repetitions. |

#### Cache Strategy (Nov 2025)
- **Primary**: Redis via ClimateCache (optional)
- **Fallback**: `requests_cache` local
- **Dynamic TTL**:
  - Forecast (future): 1h
  - Recent (past): 6h

## Implementation Summary

### Data Aggregation
- **APIs with hourly data** (MET Norway, NWS): Internal aggregation to daily
- **APIs with ready daily data** (NASA POWER, Open-Meteo): Direct use
- **Methods**: Arithmetic mean (temp/humidity/wind), Cumulative sum (precip)

### Request Management
- **Cache**: Two-layer system (Redis + fallback)
- **TTL**: Based on `Expires` header or default values
- **Retry**: Exponential backoff for HTTP 429
- **User-Agent**: Mandatory for MET Norway and NWS

### Request Limits Summary

| API | Per Second | Per Minute | Per Day | Special Policies |
|-----|------------|------------|---------|------------------|
| **MET Norway** | 20 req/s | <100 req/min | Fair use | User-Agent mandatory; HTTPS mandatory |
| **NASA POWER** | <1 req/s (rec.) | Fair use | No limit | Limit of 20 parameters per request |
| **NWS Forecast** | ≈5 req/s | Fair use | >1000 req/day (review) | User-Agent mandatory |
| **NWS Stations** | ≈5 req/s | Fair use | >1000 req/day (review) | User-Agent mandatory |
| **Open-Meteo Archive** | <10 req/s (rec.) | Fair use | ≈10,000 req/day | Cache mandatory; paid plans available |
| **Open-Meteo Forecast** | <10 req/s (rec.) | Fair use | ≈10,000 req/day | Cache mandatory; paid plans available |

### Contacts
- **MET Norway**: `support@met.no`
- **NASA POWER**: `larc-power-project@mail.nasa.gov`
- **NWS**: `api-support@noaa.gov`
- **Open-Meteo**: `support@open-meteo.com`