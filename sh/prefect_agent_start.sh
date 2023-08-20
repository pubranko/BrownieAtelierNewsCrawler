#!/bin/bash

# python仮想環境を有効化
. $HOME/.venv/bin/activate

# prefect APIの向き先を指定 兼 prefect.db生成
prefect config set PREFECT_API_URL=$PREFECT_API_URL

# prefectクラウドへのログイン
prefect cloud logout
prefect cloud login --key $PREFECT_API_KEY

# アプリディレクトリへ
cd $HOME/BrownieAtelier/app
prefect agent start --pool brownie-atelier-agent-pool

