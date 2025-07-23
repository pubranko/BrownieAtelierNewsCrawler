############
# 廃止
############
#!/bin/bash
##############################
# prefect cloud
##############################
# python仮想環境を有効化
cd $HOME/BrownieAtelierNewsCrawler/
. .venv/bin/activate
which python

# prefect APIの向き先を指定生成
# prefect config set PREFECT_API_URL=$PREFECT_API_URL

# prefectクラウドへのログイン
# prefect cloud login --key $PREFECT__API_KEY --workspace $PREFECT__WORK_SPACE
prefect cloud login --key $PREFECT__API_KEY --workspace $PREFECT__WORK_SPACE
# prefect cloud logout
# prefect cloud login --key $PREFECT__API_KEY --workspace $PREFECT__WORK_SPACE

# prefectエージェント起動
cd $HOME/BrownieAtelierNewsCrawler/app
echo "$PREFECT_RUN_SCRIPT"
python "$PREFECT_RUN_SCRIPT"
