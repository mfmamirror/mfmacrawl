FROM ubuntu:20.04

ENV POETRY_VIRTUALENVS_CREATE false
ENV PIP_NO_CACHE_DIR off
ENV PIP_DISABLE_PIP_VERSION_CHECK on
ENV PYTHONUNBUFFERED 1
ENV DEBIAN_FRONTEND="noninteractive"

RUN set -ex; \
  apt-get update; \
  apt-get install -y python3 python3-dev python3-pip libxml2-dev libxslt1-dev zlib1g-dev libffi-dev libssl-dev trickle ; \
  # cleaning up unused files \
  apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false; \
  rm -rf /var/lib/apt/lists/*

RUN pip install -U poetry

# Copy, then install requirements before copying rest for a requirements cache layer.
COPY pyproject.toml poetry.lock /tmp/
RUN set -ex; \
  cd /tmp; \
  poetry install

COPY . /app

ARG USER_ID=1001
ARG GROUP_ID=1001

RUN set -ex; \
  addgroup --gid $GROUP_ID --system containeruser; \
  adduser --system --uid $USER_ID --gid $GROUP_ID containeruser; \
  mkdir -p /var/log/mfmacrawl/ /var/lib/mfmacrawl/ ;\
  chown -R containeruser /var/log/mfmacrawl/ /var/lib/mfmacrawl/

VOLUME ["/var/log/mfmacrawl/", "/var/lib/mfmacrawl/", "/app/.scrapy/"]

WORKDIR /app

USER containeruser

CMD bin/run.sh