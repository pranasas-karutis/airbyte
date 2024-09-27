# Use a slim base image
FROM python:3.9-slim as base

# Set the working directory
WORKDIR /airbyte/integration_code

# Install build dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install poetry
RUN pip install --upgrade pip \
    && pip install poetry

# Copy pyproject.toml and poetry.lock and install dependencies
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

# Copy the rest of the application code
COPY . .

# Set the entrypoint
ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

# Label for the image
LABEL io.airbyte.name=airbyte/source-iterable-rework
