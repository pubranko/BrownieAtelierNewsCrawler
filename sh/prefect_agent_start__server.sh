#!/bin/bash
###############################################
# local環境でprefect serverを使ったtest用
###############################################
# python仮想環境を有効化
cd $PWD
. .venv/bin/activate

# prefect APIの指定が無ければデフォルト値を設定
if [ -z $PREFECT_API_URL ]; then
    export PREFECT_API_URL="http://127.0.0.1:4200/api"
fi
prefect config set PREFECT_API_URL=$PREFECT_API_URL

# prefectエージェント起動前にappディレクトリへ移動
cd $PWD/app
# prefectのワークプールの環境変数に指定がなければデフォルト値を設定
if [ -z $PREFECT__WORK_POOL ]; then
    export PREFECT__WORK_POOL="default-agent-pool"
fi
# prefectエージェント起動
prefect agent start --pool $PREFECT__WORK_POOL