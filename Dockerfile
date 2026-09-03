# 注）利用するインスタンスタイプに合わせてplatformを指定します。
ARG BUILDPLATFORM=linux/amd64
FROM --platform=${BUILDPLATFORM} public.ecr.aws/docker/library/ubuntu:jammy-20260410

RUN \
  rm -f /etc/apt/apt.conf.d/docker-clean; \
  echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
  export DEBIAN_FRONTEND=noninteractive && \
  apt-get update && \
  apt-get upgrade -y && \
  apt-get install -y gnupg curl git

# Install uv
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH=$PATH:/root/.local/bin

WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

# コンテナ起動時にマウントして利用するため、ランタイムには含めない
# ランタイムに含める場合は、コメントアウトを外して利用してください。
# COPY analyzer analyzer
# COPY models models

EXPOSE 50051
CMD ["uv", "run", "python3", "-m", "analyzer.main"]
