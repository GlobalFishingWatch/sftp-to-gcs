# ---------------------------------------------------------------------------------------
# BUILDER
# ---------------------------------------------------------------------------------------
FROM python:3.13-slim-bookworm AS builder

VOLUME ["/root/.config"]

# Use uv for high-speed installs
COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1

# Install dependencies BEFORE copying source so an edit under src/
# doesn't invalidate the cache of the (expensive) requirements-install layer. 
COPY pyproject.toml requirements.txt README.md MANIFEST.in ./
RUN uv pip install --system --upgrade pip && \
    uv pip install --system build && \
    uv pip install --system --prefix=/install -r requirements.txt

COPY src ./src
RUN uv pip install --system --prefix=/install --no-deps .

# ---------------------------------------------------------------------------------------
# PRODUCTION IMAGE
# ---------------------------------------------------------------------------------------
FROM python:3.13-slim-bookworm AS prod

ENV PYTHONUNBUFFERED=1

# Copy the pre-compiled packages from builder
COPY --from=builder /install /usr/local

WORKDIR /opt/project

# ---------------------------------------------------------------------------------------
# DEVELOPMENT IMAGE
# ---------------------------------------------------------------------------------------
FROM builder AS dev

WORKDIR /opt/project

COPY . .
RUN uv pip install --system -e .[lint,dev,build] && \
    uv pip install --system -r requirements-test.txt

# ---------------------------------------------------------------------------------------
# TEST IMAGE
# ---------------------------------------------------------------------------------------
FROM prod AS test

COPY ./requirements-test.txt .
RUN pip install -r requirements-test.txt

COPY ./tests ./tests

# Suppress all warnings during tests
# To see/address warnings, run tests in your development environment.
ENV PYTHONWARNINGS=ignore