from datetime import date
from typing import Any, Optional
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect_lib.data_models.stats_analysis_report_excel import \
    StatsAnalysisReportExcel
from prefect_lib.data_models.stats_analysis_report_input import \
    StatsAnalysisReportConst
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.stats_analysis_report_args_check_task import \
    stats_analysis_report_args_check_task
from prefect_lib.tasks.stats_analysis_report_create_task import \
    stats_analysis_report_create_task
from prefect_lib.tasks.stats_analysis_report_data_frame_task import \
    stats_analysis_report_data_frame_task
from prefect_lib.tasks.stats_analysis_report_notice_task import \
    stats_analysis_report_notice_task

StatsAnalysisReportConst.REPORT_TERM__WEEKLY
StatsAnalysisReportConst.TOTALLING_TERM__DAILY


@flow(
    name="Stats analysis report flow",
    validate_parameters=False,
)  # 入力チェックは別途行うのでFalse
def stats_analysis_report_flow(
    report_term: str, totalling_term: str, base_date: Optional[date] = None
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
            # 入力（Flowの引数）のバリデーションチェックを行い、入力のデータクラスを生成
            stats_analysis_report_input = stats_analysis_report_args_check_task(
                report_term, totalling_term, base_date
            )

            # スクレイパー情報解析用のデータフレーム管理クラスを生成
            stats_info_collect_data = stats_analysis_report_data_frame_task(
                mongo, stats_analysis_report_input
            )

            if stats_info_collect_data:
                # 対象データがある場合
                # スクレイパー情報解析レポート用Excel作成
                stats_analysis_report_excel: StatsAnalysisReportExcel = (
                    stats_analysis_report_create_task(
                        stats_analysis_report_input, stats_info_collect_data
                    )
                )

                # スクレイパー情報解析レポート用Excelの通知(送信)を実行
                stats_analysis_report_notice_task(
                    stats_analysis_report_input, stats_analysis_report_excel
                )

            else:
                # 対象データがない場合
                logger.warning(f"=== 該当データが無いため「スクレイパー情報解析レポート」の作成をキャンセルしました。")

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f"=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")


def main(**kwargs):
    stats_analysis_report_flow(**kwargs)
