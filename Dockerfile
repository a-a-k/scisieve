FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md LICENSE /app/
COPY scisieve /app/scisieve
COPY api_clients.py extractor.py models.py protocol.py snowballing.py /app/

RUN python -m pip install --upgrade pip \
    && python -m pip install .

ENTRYPOINT ["scisieve"]
CMD ["--help"]
