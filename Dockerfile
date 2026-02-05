# 注）利用するインスタンスタイプに合わせてplatformを指定します。
ARG BUILDPLATFORM=linux/amd64
FROM --platform=${BUILDPLATFORM} public.ecr.aws/docker/library/ubuntu:jammy-20251013

RUN \
  rm -f /etc/apt/apt.conf.d/docker-clean; \
  echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
  export DEBIAN_FRONTEND=noninteractive && \
  apt-get update && \
  apt-get upgrade -y && \
  apt-get install -y gnupg curl git && \
  apt-key adv --keyserver keyserver.ubuntu.com --recv f23c5a6cf475977595c89f51ba6932366a755776 && \
  echo "deb https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu jammy main" > /etc/apt/sources.list.d/python.list && \
  echo "deb-src https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu jammy main" >> /etc/apt/sources.list.d/python.list

# Install uv
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh
ENV PATH=$PATH:/root/.local/bin

WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

COPY analyzer analyzer
COPY proto proto

EXPOSE 50051
CMD ["uv", "run", "python3", "-m", "analyzer.main"]
