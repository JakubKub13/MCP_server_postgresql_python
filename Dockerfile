# ---- Builder Stage ----
# Use specific Python version for consistency
FROM python:3.11-slim AS builder

# Set working directory
WORKDIR /app

# Install dependencies (requirements.txt first for better cache utilization)
# Use virtualenv for better isolation
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Update pip and install build dependencies if needed for some libraries
RUN pip install --upgrade pip

# Copy requirements file
COPY requirements.txt .

# Install dependencies (without cache for builder stage)
# Add --no-cache-dir for smaller layer
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY server.py .

# Add this line to your Dockerfile
RUN pip install loguru

# ---- Release Stage ----
# Use the same slim base image
FROM python:3.11-slim AS release

# Install libpq5
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Copy application from builder stage
COPY --from=builder /app/server.py .

# Set PATH to run python from venv
ENV PATH="/opt/venv/bin:$PATH"
# Tell Python not to create .pyc files
ENV PYTHONDONTWRITEBYTECODE=1
# Tell Python to run in unbuffered mode (better for logging in containers)
ENV PYTHONUNBUFFERED=1

# Expose port if we want to use different transport in the future (e.g. SSE)
# Not strictly necessary for stdio
# EXPOSE 8000

# Define command that will run when container starts
# Expects database URL to be provided as an argument during `docker run`
ENTRYPOINT ["python", "server.py"]

# Example how to run the container:
# docker build -t mcp-postgres-py .
# docker run -i --rm mcp-postgres-py "postgresql://user:password@host:port/database"