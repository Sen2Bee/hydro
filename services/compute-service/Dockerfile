FROM python:3.12-slim

ENV LEGACY_BACKEND_PATH=/legacy_backend
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libexpat1 gdal-bin \
    && rm -rf /var/lib/apt/lists/*

COPY services/compute-service/requirements.txt /app/requirements.txt
COPY backend/requirements.txt /legacy_backend/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt -r /legacy_backend/requirements.txt

COPY services/compute-service/app /app/app
COPY backend /legacy_backend

CMD ["python", "app/worker.py"]
