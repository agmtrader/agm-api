#!/bin/bash

# Load environment variables first (ignoring comments and handling multiline values)
set -a
source .env
set +a

# Kill any existing process running on specified port
lsof -ti :${API_PORT} | xargs kill -9 2>/dev/null || true

# Start Gunicorn with environment variables and increased timeout
gunicorn --bind 127.0.0.1:${API_PORT} \
         --timeout 120 \
         --workers 1 \
         --threads 1 \
         run:app