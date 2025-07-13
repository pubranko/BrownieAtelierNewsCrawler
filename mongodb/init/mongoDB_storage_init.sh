#!/bin/bash
#################################################################################
# local環境でmongoDBのコンテナ起動前にStorageディレクトリを一度初期化する必要がある。
#
# 1. cdで「BrownieAtelierNewsCrawler」へ移動してください。
# 2. mongoDBのデータとログを保存するディレクトリを作成してください。
# 3. .envファイルを上記1.の直下に作成し、「BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR」に
#    対して上記2.のディレクトリ（フルパス）を設定してください。
# 4. 以下のコマンドで当シェルを実行し、mongoDB用のストレージのを初期化してください。
#    bash mongodb/init/mongoDB_storage_init.sh 
#################################################################################

# .envファイルの環境変数を読み込み
source ./.env

# ディレクトリが存在しない場合は作成
if [ ! -d "$BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR" ]; then
    echo "ディレクトリ 「'$BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR'」が存在しません。作成してください。"
else
    # ディレクトリの権限を 777 に変更
    chmod 777 "$BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR"
    echo "ディレクトリ 「'$BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR'」 の権限を 777 に変更しました。"

    if [ ! -d "$BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR/data" ]; then
        mkdir $BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR/data
        chmod 777 "$BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR/data"
        echo "ディレクトリ 「'$BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR'/data」を作成し権限を 777 に変更しました。"
    fi
    if [ ! -d "$BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR/log" ]; then
        mkdir $BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR/log
        chmod 777 "$BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR/log"
        echo "ディレクトリ 「'$BROWNIE_ATELIER_MONGO__MONGO_STORAGE_DIR'/log」 を作成し権限を 777 に変更しました。"
    fi
fi
