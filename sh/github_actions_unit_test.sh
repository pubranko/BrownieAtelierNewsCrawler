#!/bin/bash

# python仮想環境を有効化
. .venv/bin/activate
which python

# テスト用のシェルを起動
cd app
echo "$PREFECT_RUN_SCRIPT"
python "$PREFECT_RUN_SCRIPT"

