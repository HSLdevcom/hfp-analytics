# Use Python base image for development - using Debian Bookworm (stable)
FROM mcr.microsoft.com/devcontainers/python:3.10-bookworm

# Set timezone
ENV TZ="Europe/Helsinki"
ENV PYTHONUNBUFFERED=1

# Install system dependencies including GDAL for geopandas
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    wget \
    git \
    gdal-bin \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libspatialindex-dev \
    build-essential \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Install Azure Functions Core Tools (only on AMD64, skip on ARM64/Apple Silicon)
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then \
        curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > microsoft.gpg && \
        mv microsoft.gpg /etc/apt/trusted.gpg.d/microsoft.gpg && \
        sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/dotnetdev.list' && \
        apt-get update && \
        apt-get install -y azure-functions-core-tools-4 && \
        apt-get clean -y && \
        rm -rf /var/lib/apt/lists/*; \
    else \
        echo "Azure Functions Core Tools not available for $ARCH architecture. Skipping installation."; \
    fi

# Set working directory
WORKDIR /workspace

# Install Python dependencies
COPY python/requirements.txt /tmp/requirements.txt

# Set GDAL environment variables for building Python packages
ENV GDAL_CONFIG=/usr/bin/gdal-config
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN pip install --upgrade pip \
    && pip install debugpy pytest pytest-asyncio httpx \
    && pip install -r /tmp/requirements.txt

# Create .vscode-server directories with correct permissions
RUN mkdir -p /home/vscode/.vscode-server/bin \
    && mkdir -p /home/vscode/.vscode-server/data/Machine \
    && mkdir -p /home/vscode/.vscode-server/extensions \
    && mkdir -p /home/vscode/.vscode-server-insiders/bin \
    && mkdir -p /home/vscode/.vscode-server-insiders/extensions \
    && chown -R vscode:vscode /home/vscode

# The vscode user already exists in the base image
# Switch to non-root user
USER vscode
