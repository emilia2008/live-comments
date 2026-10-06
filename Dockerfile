FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

# Install dependencies first so this layer is cached when only the code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY static ./static

EXPOSE 8000

# One uvicorn process per container. To use more CPU cores, run more containers.
#  --port ${PORT:-8000}: hosting platforms (Render, Railway, ...) tell the app which port
#    to use through $PORT; docker-compose leaves it unset, so 8000 is used.
#  --ws-per-message-deflate false: compressing every tiny JSON message separately for
#    every viewer cost ~20% CPU in profiling; the bandwidth saved is small.
#  --ws-max-size 16384: comments are at most 200 characters; refuse huge frames.
# "exec" makes uvicorn the main process, so it receives the platform's stop signal.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --log-level warning --ws-per-message-deflate false --ws-max-size 16384"]
