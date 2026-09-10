# 第2段階: 構築済みベースを再利用し、Python ソース・ライブラリーを更新する。
# OS パッケージのインストールは dockerfile_base に集約し、通常の更新を軽くする。
# CONTAINER_USER は、指定する BASE_TAG のベース作成時と同じ値を渡す。
ARG BASE_TAG
FROM brownie-atelier-news-crawler-base:${BASE_TAG}

# イメージの用途を識別するメタデータ。
LABEL maintainer="BrownieAtelierNewsCrawler"

ARG CONTAINER_USER
ARG GIT_BRANCH

# ベースの実行ユーザーを継承するが、更新と実行を一般ユーザーで行う意図を明示する。
# リポジトリー・.venv・ブラウザー配置先はベース側でこのユーザーが更新可能にしてある。
USER ${CONTAINER_USER}

# アプリ用ディレクトリへ移動
WORKDIR /home/${CONTAINER_USER}/BrownieAtelierNewsCrawler

# リモートからソースを更新し、submodule を親リポジトリーが指定する版に合わせる。
# Docker はリモート Git の変更をキャッシュ判定に使わない。最新版を取り込むときは
# docker compose -f docker-compose__news-crawler-image-build.yml build --no-cache
# を使う。この指定でも FROM の構築済みベースを一からビルドし直すことはない。
RUN git config pull.rebase false
RUN git pull origin "${GIT_BRANCH}"
RUN git submodule update --recursive

# シェルに実行権限を付与
WORKDIR /home/${CONTAINER_USER}/BrownieAtelierNewsCrawler/sh
RUN chmod 755 ./*
WORKDIR /home/${CONTAINER_USER}/BrownieAtelierNewsCrawler/data
# 所有者に読み書きとディレクトリーの探索を許可し、他ユーザーの書き込みを外す。
# bind mount 使用時はホストの権限が優先されるため、その権限はホスト側で設定する。
RUN chmod -R u+rwX,go-w .
RUN ls -la

# uv はベースの /bin/uv を使い、uv.lock に従って .venv の実行時依存を同期する。
# Playwright の更新に対応する Chromium 本体も更新する（OS 依存の導入は行わない）。
# 新しい Playwright が追加の OS ライブラリーを必要とする場合はベースを再ビルドする。
WORKDIR /home/${CONTAINER_USER}/BrownieAtelierNewsCrawler
RUN uv sync --locked --no-dev \
    && .venv/bin/playwright install chromium

ENTRYPOINT []
CMD []
