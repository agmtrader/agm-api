# Use an official Python runtime as the base image
FROM python:3.9-slim

# Add labels for metadata
LABEL maintainer="AGM Technology"
LABEL name="agm-api"
LABEL version="1.0"
LABEL description=""

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Create directory for persistent database storage
RUN mkdir -p /app/src/db
RUN mkdir -p /app/cache

# Create volume mount points
VOLUME /app/src/db
VOLUME /app/cache

# Copy the application code
COPY . .

# Make run script executable
RUN chmod +x run.sh

# Define build arguments for all environment variables
# JWT Authentication
ARG JWT_SECRET_KEY
ENV JWT_SECRET_KEY=${JWT_SECRET_KEY}

# Development Mode
ARG DEV_MODE
ARG EXTRA_DEBUG
ENV DEV_MODE=${DEV_MODE}
ENV EXTRA_DEBUG=${EXTRA_DEBUG}

# API
ARG PORT
ENV PORT=${PORT}
EXPOSE ${PORT}

ENTRYPOINT ["./run.sh"]