FROM python:3.12-slim

# Install system dependencies (add ffmpeg, etc. as needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Configure pip for AWS CodeArtifact
ARG CODEARTIFACT_AUTH_TOKEN
ARG AWS_ACCOUNT_ID
RUN pip config set global.index-url https://aws:${CODEARTIFACT_AUTH_TOKEN}@plus-${AWS_ACCOUNT_ID}.d.codeartifact.us-east-1.amazonaws.com/pypi/plus-python/simple/

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir .

# Copy source code
COPY src/ ./src/

# Force unbuffered stdout/stderr for real-time logs
ENV PYTHONUNBUFFERED=1

# Entry point
CMD ["python", "-u", "-m", "src.worker"]
