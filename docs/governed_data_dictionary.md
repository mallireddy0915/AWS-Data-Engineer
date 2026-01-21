# Governed Data Dictionary (Generated)
- Source parquet: `yellow_tripdata_2025-08.parquet`
- Generated: 2026-01-21 01:38:40 UTC

| Column | Data Type | Business Meaning | Sensitivity | Owner | Steward | Retention | Quality Rule |
|---|---|---|---|---|---|---|---|
| `VendorID` | `int32` | Taxi vendor identifier | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `tpep_pickup_datetime` | `datetime64[us]` | Datetime when the meter was engaged (pickup time) | Internal | DataEngineering | DataEngineering | 730 days | NOT NULL; must be <= tpep_dropoff_datetime |
| `tpep_dropoff_datetime` | `datetime64[us]` | Datetime when the meter was disengaged (dropoff time) | Internal | DataEngineering | DataEngineering | 730 days | NOT NULL; must be >= tpep_pickup_datetime |
| `passenger_count` | `float64` | Number of passengers (reported by driver) | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `trip_distance` | `float64` | Trip distance in miles reported by taximeter | Internal | DataEngineering | DataEngineering | 730 days | >= 0 |
| `RatecodeID` | `float64` | Rate code at end of trip | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `store_and_fwd_flag` | `object` | Whether trip record was held in vehicle then forwarded | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `PULocationID` | `int32` | Pickup taxi zone ID | Internal | DataEngineering | DataEngineering | 730 days | Must exist in taxi_zone_lookup.LocationID |
| `DOLocationID` | `int32` | Dropoff taxi zone ID | Internal | DataEngineering | DataEngineering | 730 days | Must exist in taxi_zone_lookup.LocationID |
| `payment_type` | `int64` | Payment type code | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `fare_amount` | `float64` | Fare amount (time + distance) | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `extra` | `float64` | Extra charges (e.g., night, peak) | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `mta_tax` | `float64` | MTA tax | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `tip_amount` | `float64` | Tip amount | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `tolls_amount` | `float64` | Tolls amount | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `improvement_surcharge` | `float64` | Improvement surcharge | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `total_amount` | `float64` | Total amount charged | Internal | DataEngineering | DataEngineering | 730 days | >= 0 |
| `congestion_surcharge` | `float64` | Congestion surcharge | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `Airport_fee` | `float64` | TODO: Add business meaning (from TLC dictionary) | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
| `cbd_congestion_fee` | `float64` | CBD congestion fee (if present) | Internal | DataEngineering | DataEngineering | 730 days | TODO: Add quality rule |
