# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --no-deps -r <(uv pip freeze)

# Copy the source code
COPY src/ ./src/

# Install the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install -e .

# Run the application
CMD ["python", "-m", "src.main"]
