#!/usr/bin/env bash

set -euo pipefail

mkdir -p data
mkdir -p source_metadata/nyc_open_data

# Existing test datasets
# CommonPlace Features (resource: t95h-5fsr)
curl -L "https://data.cityofnewyork.us/resource/t95h-5fsr.csv?\$limit=5000" -o data/commonplace.csv
curl -L "https://data.cityofnewyork.us/api/views/t95h-5fsr" -o source_metadata/nyc_open_data/commonplace.json
# 311 Service Requests from 2010 to 2019 (resource: 76ig-c548)
curl -L "https://data.cityofnewyork.us/resource/76ig-c548.csv?\$limit=5000" -o data/nyc311.csv
curl -L "https://data.cityofnewyork.us/api/views/76ig-c548" -o source_metadata/nyc_open_data/nyc311.json
# Bi-Annual Pedestrian Counts (resource: cqsj-cfgu)
curl -L "https://data.cityofnewyork.us/resource/cqsj-cfgu.csv?\$limit=5000" -o data/ped_counts.csv
curl -L "https://data.cityofnewyork.us/api/views/cqsj-cfgu" -o source_metadata/nyc_open_data/ped_counts.json
# Schoolyard to Playgrounds (resource: bbtf-6p3c)
curl -L "https://data.cityofnewyork.us/resource/bbtf-6p3c.csv?\$limit=5000" -o data/play_areas.csv
curl -L "https://data.cityofnewyork.us/api/views/bbtf-6p3c" -o source_metadata/nyc_open_data/play_areas.json
# 2015 Street Tree Census - Tree Data (resource: uvpi-gqnh)
curl -L "https://data.cityofnewyork.us/resource/uvpi-gqnh.csv?\$limit=5000" -o data/street_trees.csv
curl -L "https://data.cityofnewyork.us/api/views/uvpi-gqnh" -o source_metadata/nyc_open_data/street_trees.json

# Additional test datasets
# NYC Wi-Fi Hotspot Locations (resource: yjub-udmw)
curl -L "https://data.cityofnewyork.us/resource/yjub-udmw.csv?\$limit=5000" -o data/wifi_hotspots.csv
curl -L "https://data.cityofnewyork.us/api/views/yjub-udmw" -o source_metadata/nyc_open_data/wifi_hotspots.json
# NYC Farmers Markets (resource: 8vwk-6iz2)
curl -L "https://data.cityofnewyork.us/resource/8vwk-6iz2.csv?\$limit=5000" -o data/farmers_markets.csv
curl -L "https://data.cityofnewyork.us/api/views/8vwk-6iz2" -o source_metadata/nyc_open_data/farmers_markets.json
# New York City Bike Routes (resource: mzxg-pwib)
curl -L "https://data.cityofnewyork.us/resource/mzxg-pwib.csv?\$limit=5000" -o data/bike_routes.csv
curl -L "https://data.cityofnewyork.us/api/views/mzxg-pwib" -o source_metadata/nyc_open_data/bike_routes.json
# Active NYC Health Code Regulated Child Care Programs (resource: gy3q-4tzp)
curl -L "https://data.cityofnewyork.us/resource/gy3q-4tzp.csv?\$limit=5000" -o data/child_care.csv
curl -L "https://data.cityofnewyork.us/api/views/gy3q-4tzp" -o source_metadata/nyc_open_data/child_care.json
# LIBRARY (resource: feuq-due4)
curl -L "https://data.cityofnewyork.us/resource/feuq-due4.csv?\$limit=5000" -o data/libraries.csv
curl -L "https://data.cityofnewyork.us/api/views/feuq-due4" -o source_metadata/nyc_open_data/libraries.json
