from datetime import date, datetime
from typing import Any, Optional

from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from dateutil.relativedelta import relativedelta
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect_lib.data_models.stats_info_collect_data import \
    StatsInfoCollectData
from prefect_lib.data_models.stats_info_collect_input import \
    StatsInfoCollectInput
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.stats_info_collect_args_check_task import \
    stats_info_collect_args_check_task
from prefect_lib.tasks.stats_info_collect_save_task import \
    stats_info_collect_save_task
from prefect_lib.tasks.stats_info_collect_task import stats_info_collect_task


@flow(
    name="Stats info collect flow",
    validate_parameters=False,
)  # 入力チェックは別途行うのでFalse
def stats_info_collect_flow(base_date: Optional[date] = None):
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
            # 引数チェックしインプットのデータクラス生成
            stats_info_collect_input: StatsInfoCollectInput = (
                stats_info_collect_args_check_task(base_date)
            )

            # クローラーログより統計情報を収集した結果をデータフレーム格納し返す。
            stats_info_collect_data: StatsInfoCollectData = stats_info_collect_task(
                mongo, stats_info_collect_input
            )

            # 上記で収集した結果を統計情報収集へ保存する。
            stats_info_collect_save_task(mongo, stats_info_collect_data)

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f"=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")


def main(**kwargs):
    stats_info_collect_flow(**kwargs)
