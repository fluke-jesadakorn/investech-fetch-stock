# Use the official Python image from the Docker Hub
FROM python:3.12.4-alpine3.19

# Set environment variables to prevent Python from writing .pyc files to disk and to buffer stdout and stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Install git
RUN apk update && apk add --no-cache git

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the working directory contents into the container at /app
COPY . .

# Expose port 8000 to the outside world
EXPOSE 8000

# Run the job by default (for jobs)
CMD ["python", "-m", "app.run_long_jobs"]