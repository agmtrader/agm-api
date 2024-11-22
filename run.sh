#!/bin/bash

# Load environment variables first (ignoring comments and handling multiline values)
export $(cat .env | xargs)

# Kill any existing process running on specified port
lsof -ti :${API_PORT} | xargs kill -9 2>/dev/null || true

# Start Gunicorn with environment variables and increased timeout
gunicorn --bind 0.0.0.0:${API_PORT} \
         --timeout 120 \
         --workers 2 \
         --threads 4 \
         run:app