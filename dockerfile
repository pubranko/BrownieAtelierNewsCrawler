ARG BASE_TAG
FROM brownie-atelier-news-crawler-base:${BASE_TAG}

#LABELでメタ情報を入れることができる
LABEL maintainer="BrownieAtelierNewsCrawler"

ARG CONTAINER_USER
ARG GIT_BRANCH

# リポジトリ一覧を更新、インストール済みのパッケージ更新
RUN echo ${CONTAINER_USER} | sudo -S apt update

# アプリ用ディレクトリへ移動
WORKDIR /home/${CONTAINER_USER}/BrownieAtelierNewsCrawler

# リモートリポジトリよりpullを実行し最新ソースを取得する。
RUN git config pull.rebase false
RUN git pull origin "${GIT_BRANCH}"
RUN git submodule update --recursive

# シェルに実行権限を付与
WORKDIR /home/${CONTAINER_USER}/BrownieAtelierNewsCrawler/sh
RUN chmod 755 ./*
WORKDIR /home/${CONTAINER_USER}/BrownieAtelierNewsCrawler/data
RUN chmod 766 ./*
RUN ls -la

# uv.lockからインストール（更新があった場合）
WORKDIR /home/${CONTAINER_USER}/BrownieAtelierNewsCrawler
RUN .venv/bin/uv sync --frozen
RUN ls -la
RUN .venv/bin/pip list

ENTRYPOINT []
CMD []
