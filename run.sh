#!/bin/bash

# Load environment variables first
export $(cat .env | xargs)

# Start Gunicorn with environment variables and increased timeout
gunicorn --bind 0.0.0.0:${API_PORT} --timeout 120 --workers 4 --threads 8 run:app