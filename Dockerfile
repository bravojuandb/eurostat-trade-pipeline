FROM python:3.12-slim

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    curl \
    p7zip-full \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python depencdecies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code + scripts (data is mounted at runtime via Docker Compose)
COPY src ./src
COPY README.md .
COPY LICENSE .

# Ensure bash scripts are executable
RUN chmod +x src/bronze/*.sh
