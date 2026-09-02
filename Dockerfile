FROM python:3.12-slim

ARG OCI_REVISION=""
ARG OCI_VERSION=""

LABEL org.opencontainers.image.source="https://github.com/onestao/wechat-hub-core"
LABEL org.opencontainers.image.revision="${OCI_REVISION}"
LABEL org.opencontainers.image.version="${OCI_VERSION}"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        fonts-noto-cjk fontconfig xdotool xclip x11-utils \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pycryptodome==3.23.0 zstandard==0.25.0 Pillow==11.3.0

COPY memory ./memory
COPY core ./core
COPY tools ./tools
COPY web ./web
COPY ai ./ai
COPY status ./status
COPY agent_console ./agent_console

CMD ["python", "-m", "core.app", "--host", "0.0.0.0", "--port", "8080", "--registry", "/app/config/wechat-runtime/accounts.json", "--require-registry", "--database", "/app/runtime/core/wechat_core.sqlite", "--sync-interval", "5", "--send-interval", "1"]
