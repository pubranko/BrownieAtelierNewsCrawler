from typing import Optional, Any
from datetime import datetime
from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect.task_runners import SequentialTaskRunner

from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.end_task import end_task
from prefect_lib.flows.common_flow import common_flow
from prefect_lib.tasks.scraper_pattern_report_args_check_task import scraper_pattern_report_args_check_task
from prefect_lib.tasks.scraper_pattern_report_data_frame_task import scraper_pattern_report_data_frame_task
from prefect_lib.tasks.scraper_pattern_report_header_task import scraper_pattern_report_header_task
from prefect_lib.tasks.scraper_pattern_report_body_task import scraper_pattern_report_body_task
from prefect_lib.tasks.scraper_pattern_report_notice_task import scraper_pattern_report_notice_task
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel


@flow(
    flow_run_name='[STATS_003] Scraper pattern info report flow',
    task_runner=SequentialTaskRunner())
@common_flow
def scraper_pattern_report_flow(report_term: str, base_date: Optional[datetime] = None):

    # ロガー取得
    logger = get_run_logger()   # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    if init_task_result.get_state().is_completed():
        mongo: MongoModel = init_task_result.result()

        try:
            # 入力（Flowの引数）のデータクラス、スクレイパー情報解析用のデータフレーム管理クラスを取得
            # scraper_pattern_report_input, scraper_pattern_report_data  = scraper_pattern_report_init_task(mongo, report_term, base_date)

            # 入力（Flowの引数）のバリデーションチェックを行い、入力のデータクラスを生成
            scraper_pattern_report_input = scraper_pattern_report_args_check_task(report_term, base_date)
            # スクレイパー情報解析用のデータフレーム管理クラスを生成
            scraper_pattern_report_data = scraper_pattern_report_data_frame_task(mongo, scraper_pattern_report_input)

            # スクレイパー情報集計結果報告用のワークブックの新規作成
            workbook = Workbook()
            any: Any = workbook.active  # アクティブなワークシートを選択
            worksheet: Worksheet = any

            # スクレイパー情報解析レポート用Excelの見出し編集
            scraper_pattern_report_header_task(worksheet)
            # スクレイパー情報解析レポート用Excelの編集
            scraper_pattern_report_body_task(scraper_pattern_report_data, worksheet)
            # スクレイパー情報解析レポート用Excelの通知を実行
            scraper_pattern_report_notice_task(scraper_pattern_report_input, workbook)

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f'=== {e}')
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f'=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。')
