-- Create a table to store descriptive data of 63 cities and provinces
CREATE TABLE vn_weather_data.places_info (
  place_id STRING NOT NULL,
  adm_area1 STRING,
  adm_area2 STRING,
  country STRING NOT NULL,
  lat STRING NOT NULL,
  lon STRING NOT NULL,
  name STRING NOT NULL,
  timezone STRING,
  type STRING
);

-- Create a table to store hourly weather data of the places
CREATE TABLE vn_weather_data.hourly_weather_data (
  id STRING NOT NULL,
  place_id STRING NOT NULL,
  cloud INT64,
  condition STRING,
  feelslike_c FLOAT64,
  feelslike_f FLOAT64,
  gust_kph FLOAT64,
  gust_mph FLOAT64,
  humidity INT64,
  is_day BOOL,
  last_updated TIMESTAMP NOT NULL,
  last_updated_epoch INT64,
  precip_in FLOAT64,
  precip_mm FLOAT64,
  pressure_in FLOAT64,
  pressure_mb FLOAT64,
  temp_c FLOAT64,
  temp_f FLOAT64,
  uv FLOAT64,
  vis_km FLOAT64,
  vis_miles FLOAT64,
  wind_degree INT64,
  wind_dir STRING,
  wind_kph FLOAT64,
  wind_mph FLOAT64
);