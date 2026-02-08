FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

COPY ingest_data.py ingest_data_2.py .

ENTRYPOINT ["sh", "-c", "python ingest_data.py \"$@\" && python ingest_data_2.py \"$@\"", "--"]