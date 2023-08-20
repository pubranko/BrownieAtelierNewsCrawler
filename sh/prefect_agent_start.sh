#!/bin/bash

# python仮想環境を有効化
. $HOME/.venv/bin/activate

# prefect APIの向き先を指定生成
prefect config set PREFECT_API_URL=$PREFECT__API_URL

# prefectクラウドへのログイン
prefect cloud logout
prefect cloud login --key $PREFECT__API_KEY

# アプリディレクトリへ
cd $HOME/BrownieAtelier/app
prefect agent start --pool brownie-atelier-agent-pool

