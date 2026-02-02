FROM python:3.12-slim

# System dependencies required by your bash workers
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    curl \
    p7zip-full \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code + scripts
COPY src ./src
COPY data ./data
COPY README.md .
COPY LICENSE .

# Ensure bash scripts are executable (defensive)
RUN chmod +x src/bronze/*.sh

# Default: show help (safe)
CMD ["python", "-m", "src.bronze.fetch", "--help"]