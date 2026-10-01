FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.16 /uv /bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY max_mirror ./max_mirror
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1 MAX_DATA_DIR=/data
RUN useradd --uid 1000 app && mkdir -m 700 /data && chown app /data
USER app
EXPOSE 8080
CMD ["python", "-m", "max_mirror", "run"]
