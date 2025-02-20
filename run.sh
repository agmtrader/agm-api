#!/bin/bash

# Start Gunicorn with environment variables and increased timeout
if [ -f .env ]; then
    while IFS= read -r line || [ -n "$line" ]; do
        if [[ $line =~ ^[^#] ]]; then
            eval "export $line"
        fi
    done < .env
fi

gunicorn --bind 0.0.0.0:${API_PORT} --timeout 120 --workers 4 --threads 8 run:app