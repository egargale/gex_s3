# Stage 1: Build stage
FROM python:3.12-slim-bookworm AS builder

# Set working directory
WORKDIR /app
ARG PYDANTIC_VERSION=2.5.3
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
# Explicit CPU architecture optimizations
ENV CFLAGS="-march=native -O3"
ENV LDFLAGS="-flto -fuse-ld=gold"
# Copy only requirements.txt first to leverage Docker cache
COPY requirements.txt .
# Install build dependencies for lxml and other packages
RUN set -eux \
    && apt-get update && apt-get install -y  --no-install-recommends \
        build-essential \
        gfortran \
        pkg-config \
        libopenblas-dev \
        liblapack-dev \
        python3-dev \
        curl \
        cargo \
        libxml2-dev \
        libxslt1-dev \
        libpq-dev \
        libffi-dev \
        libopenblas64-openmp-dev \
        libatlas3-base \
    && python -m pip install --upgrade pip \
    && python -m venv /opt/venv \
    &&  /opt/venv/bin/pip install --no-cache-dir -r requirements.txt \
    # Cleanup build deps
    && apt-get purge --auto-remove --yes \
        build-essential \
    && apt-get clean \
    && rm --recursive --force /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Stage 2: Final stage
FROM python:3.12-slim-bookworm

# Set working directory
WORKDIR /app

ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Install runtime dependencies & clean in single layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    libopenblas-dev \
    liblapack-dev \
    libxml2-dev \
    libxslt1-dev \
    libpq-dev \
    libffi-dev \
    libopenblas64-openmp-dev \
    libatlas3-base \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Copy Python env from builder to clean container image
COPY --from=builder /opt/venv /opt/venv

# Copy application code
COPY . .

# Expose port (default is 80, but can be overridden via .env file)
EXPOSE 8000 

# Activate virtual environment and run the app
# Note: Use shell form to allow environment variable expansion
CMD ["/bin/sh", "-c", "/opt/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --loop uvloop --http httptools --timeout-keep-alive 60 --no-access-log"]
