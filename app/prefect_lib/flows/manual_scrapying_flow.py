from datetime import datetime

from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect_lib.flows import START_TIME
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.news_clip_master_save_task import news_clip_master_save_task
from prefect_lib.tasks.scrapying_task import scrapying_task


@flow(name="Manual scrapying flow")
def manual_scrapying_flow(
    domain: str | None = None,  # domainによる指定がある場合に使用
    target_start_time_from: datetime | None = None,  # 対象の時間帯がある場合に使用
    target_start_time_to: datetime | None = None,
    urls: list[str] | None = None,  # 特定のURLのみ処理を実施したい場合に指定
    # 後続処理実施指定: True->スクレイピング後の後続処理あり
    # False->スクレイピング後の後続処理なし
    following_processing_execution: bool = False,
):
    init_flow()

    # ロガー取得
    logger = get_run_logger()  # PrefectLogAdapter
    # 初期処理
    init_task_instance: PrefectFuture = init_task.submit()
    # 実行結果が返ってくるまで待機し、戻り値を保存。
    # ※タスクのステータスをresultを受け取る前に判定してもPendingとなる。
    # インスタンスのステータスはリアルタイムで更新されているので注意。
    init_task_result = init_task_instance.result()

    if init_task_instance.state.is_completed():
        mongo: MongoModel = init_task_result

        try:
            # 引数で指定されたクロール結果のスクレイピングを実施
            scrapying_task(mongo, domain, urls, target_start_time_from, target_start_time_to)

            if following_processing_execution:
                # 後続処理実施指定がある場合、スクレイピング結果をニュースクリップマスターへ保存
                news_clip_master_save_task(mongo, domain, START_TIME, START_TIME)

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error("=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")


def main(**kwargs):
    manual_scrapying_flow(**kwargs)
