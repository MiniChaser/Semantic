# Use Python 3.11 as base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies including cron
RUN apt-get update && apt-get install -y \
    cron \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Copy project files
COPY . .

# Install Python dependencies using uv
RUN uv sync

# Create log directory for script outputs
RUN mkdir -p /var/log/semantic

# Copy and setup Docker scripts
COPY docker/ /app/docker/
RUN chmod +x /app/docker/*.sh /app/docker/*.py

# Expose any ports if needed (optional)
# EXPOSE 8000

# Start the service
CMD ["/app/docker/start.sh"]