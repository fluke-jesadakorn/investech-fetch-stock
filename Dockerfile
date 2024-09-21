# Stage 1: Build dependencies
FROM python:3.12.4-alpine3.19 AS build

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install build dependencies
RUN apk --no-cache add git gcc musl-dev libffi-dev

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies in a virtual environment
COPY requirements.txt .

# Create virtual environment and install dependencies into .venv
RUN python -m venv /app/.venv \
    && /app/.venv/bin/pip install --no-cache-dir -r requirements.txt

# Stage 2: Create final image
FROM python:3.12.4-alpine3.19

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/.venv/lib/python3.12/site-packages"

# Set the working directory
WORKDIR /app

# Install only the system dependencies needed at runtime
RUN apk --no-cache add git

# Copy virtual environment from the build stage
COPY --from=build /app/.venv /app/.venv

# Copy the application code
COPY . .

# Expose port 8080
EXPOSE 8080

# Run the application using the virtual environment
CMD ["/app/.venv/bin/python", "-m", "app.main"]
