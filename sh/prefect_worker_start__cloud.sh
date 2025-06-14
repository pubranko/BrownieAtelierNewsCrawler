#!/bin/bash
##############################
# prefect cloud
##############################
# python仮想環境を有効化
# cd $HOME/BrownieAtelierNewsCrawler/
. .venv/bin/activate
which python
echo $PWD
echo $PREFECT_HOME
echo $PREFECT_API_URL
echo $PREFECT__WORK_POOL

# prefect APIの向き先を指定生成
# prefect config set PREFECT_API_URL=$PREFECT_API_URL

# prefectクラウドへのログイン
prefect cloud login --key $PREFECT__API_KEY --workspace $PREFECT__WORK_SPACE
prefect cloud logout
prefect cloud login --key $PREFECT__API_KEY --workspace $PREFECT__WORK_SPACE

# prefectワーカー起動
cd $HOME/BrownieAtelierNewsCrawler/app
prefect worker start --pool $PREFECT__WORK_POOL