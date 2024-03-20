from typing import Any

from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect.states import State
from prefect.task_runners import SequentialTaskRunner
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.mongo_common_task import mongo_common_task
from prefect_lib.tasks.mongo_delete_task import mongo_delete_task


@flow(name="Mongo delete selector flow", task_runner=SequentialTaskRunner())
def mongo_delete_selector_flow(
    collections_name: list[str],
    period_month_from: int,  # 月次エクスポートを行うデータの基準年月  ex)0 -> 当月, 1 => 前月
    period_month_to: int,  # 月次エクスポートを行うデータの基準年月
    crawler_response__registered: bool = True,  # crawler_responseの場合、登録済みになったレコードのみ削除する場合True、登録済み以外のレコードも含めて削除する場合False
):
    init_flow()

    # ロガー取得
    logger = get_run_logger()  # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    any: Any = init_task_result.get_state()
    state: State = any
    if state.is_completed():
        mongo: MongoModel = init_task_result.result()

        try:
            # mongo操作Flowの共通処理
            dir_path, period_from, period_to = mongo_common_task(
                "", "", period_month_from, period_month_to
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
