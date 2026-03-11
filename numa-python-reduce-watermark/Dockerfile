FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

RUN apt update && apt install -y git

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY main.py .

CMD ["uv", "run", "main.py"]

