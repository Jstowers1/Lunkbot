FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg libopus0 git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

#Install deps first for layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#Copy application code.
COPY jellyfin_db.py bot.py ./
COPY scripts/ ./scripts/

#Run as non-root for defense-in-depth.
RUN useradd -r -s /bin/false lunkbot && chown -R lunkbot:lunkbot /app
USER lunkbot

CMD ["python3", "-u", "bot.py"]
