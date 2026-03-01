FROM python:3.12-slim

# Install system dependencies including Playwright/Chromium requirements
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    # Playwright/Chromium dependencies
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxcomposite1 \
    libxrandr2 \
    libxdamage1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libxshmfence1 \
    libgbm1 \
    libgtk-3-0 \
    libx11-xcb1 \
    libxcb-dri3-0 \
    libdrm2 \
    libxss1 \
    fonts-liberation \
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

# Install Playwright browsers (Chromium only to minimize size)
RUN playwright install chromium

# Copy source code
COPY src/ ./src/

# Force unbuffered stdout/stderr for real-time logs
ENV PYTHONUNBUFFERED=1

# Entry point
CMD ["python", "-u", "-m", "src.worker"]
