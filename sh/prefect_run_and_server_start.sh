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

# Prefect Serverをバックグラウンドで起動
prefect server start &

# サーバー起動待機（5秒）
sleep 5

# フロー実行
# prefect deployment run --name YOUR_DEPLOYMENT_NAME --project my-project
cd app
export PYTHONPATH=$PWD:$PYTHONPATH
python prefect_lib/batch_exec/scraper_info_uploader_exec.py
