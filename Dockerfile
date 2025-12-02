FROM python:3.12-slim

# Install system dependencies (add ffmpeg, etc. as needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir .

# TODO: Install plus-engine and feaas-core as dependencies
# Option 1: Git deps
# RUN pip install git+https://github.com/org/feaas-core.git@main
# RUN pip install git+https://github.com/org/plus-engine.git@main
#
# Option 2: CodeArtifact (uncomment and configure)
# ARG CODEARTIFACT_AUTH_TOKEN
# RUN pip config set global.index-url https://aws:${CODEARTIFACT_AUTH_TOKEN}@domain-owner.d.codeartifact.region.amazonaws.com/pypi/repo/simple/
# RUN pip install feaas-core plus-engine

# Copy source code
COPY src/ ./src/

# Entry point
CMD ["python", "-m", "src.worker"]
