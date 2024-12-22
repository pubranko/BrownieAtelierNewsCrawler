#!/bin/bash

# シェル内でエラーが発生した場合、呼び出し元のコンテナーもエラー扱いとなるよう設定
set -e  

# python仮想環境を有効化
. .venv/bin/activate
which python

# テスト用のシェルを起動
cd app
echo "$PREFECT_RUN_SCRIPT"
python "$PREFECT_RUN_SCRIPT"

