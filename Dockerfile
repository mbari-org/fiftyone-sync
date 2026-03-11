# FiftyOne Sync Service
FROM python:3.12-slim

WORKDIR /app

# Install system deps for Pillow / image handling
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libjpeg-dev zlib1g-dev \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
EXPOSE 8001
CMD ["uvicorn", "src.app.main:app", "--host", "0.0.0.0", "--port", "8001"]
