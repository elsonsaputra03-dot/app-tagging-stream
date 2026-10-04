FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml README.md ./
COPY apptag ./apptag
COPY dictionary ./dictionary
RUN pip install --no-cache-dir .
ENTRYPOINT ["apptag"]
