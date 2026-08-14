# E-Commerce Data Pipeline - Multi-stage Docker Build
# Stage 1: Builder - Install dependencies
FROM python:3.10-slim as builder

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
RUN pip install uv

# Copy project files
COPY pyproject.toml uv.lock* ./
COPY . .

# Install Python dependencies
RUN uv sync --frozen --no-dev


# Stage 2: Runtime - Minimal production image
FROM python:3.10-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY --from=builder /app/src ./src
COPY --from=builder /app/config ./config
COPY --from=builder /app/sql ./sql

# Set environment variables
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Create non-root user for security
RUN useradd -m -u 1000 pipeline && chown -R pipeline:pipeline /app
USER pipeline

# Default command: Run Prefect flow
ENTRYPOINT ["python", "-m", "src.flows.prefect_flow"]
CMD ["--run-date", "$(date +%Y-%m-%d)"]

# Health check
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import src.utils.logger; print('OK')" || exit 1
