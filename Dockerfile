# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

# Copy and install deps first (better layer caching)
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy app
COPY herd_behavior_alerter ./herd_behavior_alerter

EXPOSE 8000

ENV HBA_HOST=0.0.0.0 \
    HBA_PORT=8000 \
    HBA_API_PREFIX=/api

CMD ["uvicorn", "herd_behavior_alerter.app:app", "--host", "0.0.0.0", "--port", "8000"]
