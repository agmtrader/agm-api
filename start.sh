#!/bin/bash

# Load environment variables first
export $(cat .env | xargs)

# Kill any existing process running on specified port
lsof -ti :${API_PORT} | xargs kill -9 2>/dev/null || true

# Start Gunicorn with environment variables
gunicorn --bind 0.0.0.0:${API_PORT} run:app