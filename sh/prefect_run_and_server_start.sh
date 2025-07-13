#!/bin/bash
######################################################################
# Prefect Serverを起動し、指定されたスクリプトを実行するシェルスクリプト
######################################################################
# python仮想環境を有効化
. .venv/bin/activate

echo "Python path: $(which python)"
echo "Current dir: $PWD"
echo "Prefect home: $PREFECT_HOME"
echo "API URL: $PREFECT_API_URL"
echo "Work pool: $PREFECT__WORK_POOL"

# Prefect Serverをバックグラウンドで起動
prefect server start &

# サーバー起動待機
sleep $SLEEP_TIME_AFTER_PREFECT_SERVER_STARTUP

cd app
echo "フロー実行: $PREFECT_RUN_SCRIPT"
export PYTHONPATH=$PWD:$PYTHONPATH
# python prefect_lib/batch_exec/scraper_info_uploader_exec.py
python $PREFECT_RUN_SCRIPT
