
from typing import Any, Final
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect.task_runners import SequentialTaskRunner
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.end_task import end_task
from prefect_lib.flows.common_flow import common_flow
from prefect_lib.tasks.regular_observation_controller_update_task import regular_observation_controller_update_task
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect_lib.flows import START_TIME


@flow(
    flow_run_name='[ENTRY_002] Regular observation controller update Flow',
    task_runner=SequentialTaskRunner())
@common_flow
def regular_observation_controller_update_flow(spiders_name: str, register: str):

    # ロガー取得
    logger = get_run_logger()   # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    if init_task_result.get_state().is_completed():
        mongo: MongoModel = init_task_result.result()

        try:
            regular_observation_controller_update_task(mongo, spiders_name, register)

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f'=== {e}')
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f'=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。')
