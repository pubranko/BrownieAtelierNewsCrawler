#!/bin/bash
###############################################
# local環境でprefect serverを使ったtest用
###############################################
# python仮想環境を有効化
cd $PWD
. .venv/bin/activate
export PREFECT_API_URL="http://127.0.0.1:4200/api"

echo "Python path: $(which python)"
echo "Current dir: $PWD"
echo "Prefect home: $PREFECT_HOME"
echo "API URL: $PREFECT_API_URL"
echo "Work pool: $PREFECT__WORK_POOL"

# Prefect Serverをデタッチモードで起動
prefect server start --detach

# サーバー起動待機（5秒）
sleep 5

# フロー実行（deployment名は適宜変更）
prefect deployment run --name YOUR_DEPLOYMENT_NAME --project my-project
あとで
