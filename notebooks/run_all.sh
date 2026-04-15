#!/bin/bash
set -e

echo "Running notebooks 00-05 on Google Cloud Run"

jupyter nbconvert --to notebook --execute notebooks/00_Initial_setup.ipynb --output-dir=notebooks/
jupyter nbconvert --to notebook --execute notebooks/01_flight_ingestion.ipynb --output-dir=notebooks/
jupyter nbconvert --to notebook --execute notebooks/02_weather_ingestion.ipynb --output-dir=notebooks/
jupyter nbconvert --to notebook --execute notebooks/03_flight_processing.ipynb --output-dir=notebooks/
jupyter nbconvert --to notebook --execute notebooks/04_weather_processing.ipynb --output-dir=notebooks/
jupyter nbconvert --to notebook --execute notebooks/05_prediction_delay.ipynb --output-dir=notebooks/

echo "All notebooks executed successfully!"