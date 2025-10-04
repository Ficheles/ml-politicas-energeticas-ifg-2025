# Start from a slim Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /usr/app/dbt

# Install dbt and the specific adapter you need (e.g., ClickHouse)
# Using a requirements.txt file is a best practice
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your entire dbt project into the container
COPY . .

# (Optional) Tell dbt where to find your profiles file
ENV DBT_PROFILES_DIR=/root/.dbt
