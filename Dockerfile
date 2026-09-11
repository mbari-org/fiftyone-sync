# FiftyOne Sync Service
FROM python:3.12-slim

# Create a non-root user
ARG APP_UID=1000
ARG APP_GID=1000
RUN groupadd -r -g "${APP_GID}" appgroup && \
    useradd -r -u "${APP_UID}" -g appgroup appuser

WORKDIR /app

# Install system deps for Pillow / image handling and ffmpeg for cropping (image + video)
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libjpeg-dev zlib1g-dev \
    ffmpeg \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . .

# Ensure appuser owns the /app directory
RUN chown -R "${APP_UID}:${APP_GID}" /app

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
EXPOSE 8001

USER ${APP_UID}

CMD ["uvicorn", "src.app.main:app", "--host", "0.0.0.0", "--port", "8001"]
