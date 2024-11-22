# Use an official Python runtime as the base image
FROM python:3.9-slim

# Add labels for metadata
LABEL maintainer="aguilarcarboni"
LABEL name="agm-api"
LABEL version="1.0"
LABEL description="AGM API"

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Copy the .env file into the container
COPY .env .
COPY run.sh .
RUN chmod +x run.sh

ENTRYPOINT ["./run.sh"]