FROM python:3.10-alpine

ARG REPO_DIR

EXPOSE 8000

ENV SPECTRED_HOST_1=n.mainnet-dnsseed-1.spectre-network.org:18110
ENV SQL_URI=postgresql+asyncpg://postgres:password@postgresql:5432/postgres
ARG version
ENV VERSION=$version

RUN apk --no-cache add \
  git \
  gcc \
  libc-dev \
  build-base \
  linux-headers \
  libpq-dev \
  dumb-init

RUN pip install \
  pipenv

RUN addgroup -S -g 27182 api \
  && adduser -h /app -S -D -g '' -G api -u 27182 api

WORKDIR /app

USER api

COPY --chown=api:api . /app

RUN pipenv install --deploy

ENTRYPOINT ["/usr/bin/dumb-init", "--"]

CMD pipenv run python main.py
