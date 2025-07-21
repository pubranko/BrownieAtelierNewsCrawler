from datetime import date
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.mongo_common_task import mongo_common_task
from prefect_lib.tasks.mongo_delete_task import mongo_delete_task


@flow(name="Mongo delete selector flow")
def mongo_delete_selector_flow(
    collections_name: list[str],
    period_date_from: date,  # 月次エクスポートを行うデータの基準年月日
    period_date_to: date,  # 月次エクスポートを行うデータの基準年月日
    crawler_response__registered: bool = True,  # crawler_responseの場合、登録済みになったレコードのみ削除する場合True、登録済み以外のレコードも含めて削除する場合False
):
    init_flow()

    # ロガー取得
    logger = get_run_logger()  # PrefectLogAdapter
    # 初期処理
    init_task_instance: PrefectFuture = init_task.submit()
    # 実行結果が返ってくるまで待機し、戻り値を保存。 
    #   ※タスクのステータスをresultを受け取る前に判定してもPendingとなる。インスタンスのステータスはリアルタイムで更新されているので注意。
    init_task_result = init_task_instance.result()

    if init_task_instance.state.is_completed():
        mongo: MongoModel = init_task_result

        try:
            # mongo操作Flowの共通処理
            dir_path, period_from, period_to = mongo_common_task(
                "", "", period_date_from, period_date_to
            )

            mongo_delete_task(
                mongo,
                period_from,
                period_to,
                collections_name,
                crawler_response__registered,
            )

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f"=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")


def main(**kwargs):
    mongo_delete_selector_flow(**kwargs)
