#!/bin/bash

# python仮想環境を有効化
cd $HOME/BrownieAtelier/app
python3 -m pipenv shell

# prefect APIの向き先を指定生成
prefect config set PREFECT_API_URL=$PREFECT__API_URL

# prefectクラウドへのログイン
prefect cloud login --key $PREFECT__API_KEY --workspace $PREFECT__WORK_SPACE
prefect cloud logout
prefect cloud login --key $PREFECT__API_KEY --workspace $PREFECT__WORK_SPACE

# prefectエージェント起動
prefect agent start --pool brownie-atelier-agent-pool

