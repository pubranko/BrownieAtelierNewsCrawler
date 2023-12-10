#!/bin/bash

# python仮想環境を有効化
cd $HOME/BrownieAtelier/app
# docker内では使えない。「pipenv run 」を付けることでpipenvの仮想環境内のコマンドが使用可能となる。
# python3 -m pipenv shell

# prefect APIの向き先を指定生成
pipenv run prefect config set PREFECT_API_URL=$PREFECT__API_URL

# prefectクラウドへのログイン
pipenv run prefect cloud login --key $PREFECT__API_KEY --workspace $PREFECT__WORK_SPACE
pipenv run prefect cloud logout
pipenv run prefect cloud login --key $PREFECT__API_KEY --workspace $PREFECT__WORK_SPACE

# prefectエージェント起動
pipenv run prefect agent start --pool brownie-atelier-agent-pool

