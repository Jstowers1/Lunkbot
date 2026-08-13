FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg libopus0 git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

#Model cache inside /app so lunkbot user can access it.
ENV HF_HOME=/app/.cache

#Install deps first for layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#Pre-download whisper-tiny model so it doesn't fetch on startup.
RUN python3 -c "from faster_whisper import WhisperModel; WhisperModel('tiny', device='cpu', compute_type='int8')"

#Copy application code.
COPY jellyfin_db.py bot.py ./
COPY scripts/ ./scripts/

#Run as non-root for defense-in-depth.
RUN useradd -r -s /bin/false lunkbot && chown -R lunkbot:lunkbot /app
USER lunkbot

CMD ["python3", "-u", "bot.py"]
