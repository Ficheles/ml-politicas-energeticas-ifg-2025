# Start from a modern, stable Airflow image. This is highly recommended.
FROM apache/airflow:2.8.1

# Switch to the root user to install software
USER root

# --- STEP 1: Update package lists ---
# If this fails, there is a network or repository issue.
RUN apt-get update

# --- STEP 2: Install system dependencies ---
# This installs the tools needed for Python packages and for Docker.
# If this fails, one of the package names is incorrect for this OS.
RUN apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    python3-dev \
    docker.io \
    docker-compose

# --- STEP 3: Add 'airflow' user to the 'docker' group ---
# This is a crucial permissions step.
RUN usermod -aG docker airflow

# --- STEP 4: Clean up ---
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the non-root airflow user
USER airflow