# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Install required packages for downloading and extracting dbt Cloud CLI
RUN apt-get update && apt-get install -y \
    curl \
    jq \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install dbt Cloud CLI
RUN BIN_DIR="/usr/local/bin" && \
    CLI_VERSION=$(curl -s "https://api.github.com/repos/dbt-labs/dbt-cli/releases/latest" | jq -r '.tag_name' | sed 's/v//') && \
    curl -LO "https://github.com/dbt-labs/dbt-cli/releases/download/v${CLI_VERSION}/dbt_${CLI_VERSION}_linux_amd64.tar.gz" && \
    tar -xzf "dbt_${CLI_VERSION}_linux_amd64.tar.gz" -C "$BIN_DIR" dbt && \
    rm "dbt_${CLI_VERSION}_linux_amd64.tar.gz"


# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT []

# Run the application
CMD ["python", "-m", "src.main"]