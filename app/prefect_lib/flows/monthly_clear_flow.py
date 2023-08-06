from datetime import datetime, date, time
from dateutil.relativedelta import relativedelta
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect.task_runners import SequentialTaskRunner

from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.end_task import end_task
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.mongo_delete_task import mongo_delete_task
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.collection_models.asynchronous_report_model import AsynchronousReportModel
from shared.settings import TIMEZONE

@flow(
    name='monthly clear flow',
    task_runner=SequentialTaskRunner())
def monthly_clear_flow():
    init_flow()

    # ロガー取得
    logger = get_run_logger()   # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    if init_task_result.get_state().is_completed():
        mongo: MongoModel = init_task_result.result()

        try:
            # ３ヶ月前の１ヶ月分を削除期間とする。
            # ３ヶ月前の月初00:00:00.000000
            period_from: datetime = datetime.combine(
                date.today() - relativedelta(months=3),
                time.min,
                TIMEZONE
                ) + relativedelta(day=1)
            # ３ヶ月前の月末23:59:59.999999
            period_to: datetime = datetime.combine(
                date.today() - relativedelta(months=3),
                time.max,
                TIMEZONE
                ) + relativedelta(day=99)

            # のデータを一括削除
            mongo_delete_task(mongo, period_from, period_to, [
                CrawlerResponseModel.COLLECTION_NAME,
                CrawlerLogsModel.COLLECTION_NAME,
                AsynchronousReportModel.COLLECTION_NAME],
                True)

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f'=== {e}')
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f'=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。')
