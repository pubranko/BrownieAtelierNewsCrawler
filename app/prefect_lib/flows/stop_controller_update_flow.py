from typing import Any, Final
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.controller_model import \
    ControllerModel
from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.stop_controller_update_args_check_task import \
    stop_controller_update_args_check_task
from prefect_lib.tasks.stop_controller_update_task import \
    stop_controller_update_task


@flow(
    name="Stop Controller Update Flow",
    validate_parameters=False,
)  # 入力チェックは別途行うのでFalse
def stop_controller_update_flow(domain: str, command: str, destination: str):
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
            stop_controller_update_input = stop_controller_update_args_check_task(
                domain, command, destination
            )
            stop_controller_update_task(stop_controller_update_input, mongo)
        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)
    else:
        logger.error(f"=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")


def main(**kwargs):
    stop_controller_update_flow(**kwargs)
