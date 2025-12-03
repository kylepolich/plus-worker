FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy pyproject.toml first for better caching
COPY pyproject.toml .

# Install Python dependencies using BuildKit secret for CodeArtifact token
# The secret is mounted only during this RUN command and not stored in the image
ARG AWS_ACCOUNT_ID
RUN --mount=type=secret,id=codeartifact_token \
    pip config set global.index-url https://aws:$(cat /run/secrets/codeartifact_token)@plus-${AWS_ACCOUNT_ID}.d.codeartifact.us-east-1.amazonaws.com/pypi/plus-python/simple/ && \
    pip install --no-cache-dir . && \
    pip config unset global.index-url

# Copy source code
COPY src/ ./src/

# Force unbuffered stdout/stderr for real-time logs
ENV PYTHONUNBUFFERED=1

# Entry point
CMD ["python", "-u", "-m", "src.worker"]
