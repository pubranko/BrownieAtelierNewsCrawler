from prefect import get_run_logger, task
from prefect_lib.flows import START_TIME
from BrownieAtelierStorage.settings import AZURE_STORAGE__CONNECTION_STRING
from BrownieAtelierStorage.models.controller_blob_model import \
    ControllerBlobModel

"""
BrownieAtelierApp、BrownieAtelierMongoコンテナーを停止させるためのトリガーを起動させる。
"""

@task
def container_end_task():
    """prefectの初期処理専用タスク"""

    logger = get_run_logger()
    logger.info(f"=== start_time : {START_TIME.isoformat()}")

    # コンテナーを停止トリガーさせる。
    #   azure functions BLOBトリガーを動かすためのBLOBファイルを削除＆作成を実行する。
    #   ※テスト環境の場合は実行しない。AZURE_STORAGE__CONNECTION_STRINGに値がある＝本番環境。
    if AZURE_STORAGE__CONNECTION_STRING:
        logger.info('=== BLOB TRIGGERを起動させコンテナーを停止させます。')
        controller_blob_model = ControllerBlobModel()
        controller_blob_model.delete_blob()
        controller_blob_model.upload_blob()
