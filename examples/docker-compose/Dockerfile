FROM python:3.13-slim

WORKDIR /app

# Install the package and its runtime dependencies
# Build context is the repo root (set in docker-compose.yml)
COPY pyproject.toml README.md ./
COPY dlstorage ./dlstorage
RUN pip install --no-cache-dir fastapi uvicorn && pip install --no-cache-dir -e .

COPY examples/docker-compose/app.py ./app.py

EXPOSE 7001 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "debug"]
