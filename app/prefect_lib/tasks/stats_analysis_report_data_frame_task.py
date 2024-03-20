from typing import Any, Union

from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.stats_info_collect_model import \
    StatsInfoCollectModel
from prefect import get_run_logger, task
from prefect_lib.data_models.stats_analysis_report_input import \
    StatsAnalysisReportInput
from prefect_lib.data_models.stats_info_collect_data import \
    StatsInfoCollectData
from prefect_lib.flows import START_TIME
from pymongo.cursor import Cursor


@task
def stats_analysis_report_data_frame_task(
    mongo: MongoModel, stats_analysis_report_input: StatsAnalysisReportInput
) -> Union[StatsInfoCollectData, None]:
    """
    mongoDBより指定期間内の統計情報集計レコードを取得しデータフレームを生成する。
    生成したデータフレームを返す。
    ただし該当データがゼロ件の場合Noneを返す。
    """

    logger = get_run_logger()  # PrefectLogAdapter

    stats_info_collect_model = StatsInfoCollectModel(mongo)
    base_date_from, base_date_to = stats_analysis_report_input.base_date_get(START_TIME)

    conditions: list = []
    conditions.append({StatsInfoCollectModel.START_TIME: {"$gte": base_date_from}})
    conditions.append({StatsInfoCollectModel.START_TIME: {"$lt": base_date_to}})
    log_filter: Any = {"$and": conditions}

    record_count: int = stats_info_collect_model.count(filter=log_filter)

    stats_info_collect_records: Cursor = stats_info_collect_model.find(
        filter=log_filter, projection={"_id": 0, "parameter": 0}  # 不要項目を除外
    )

    logger.info(f"=== 統計情報レポート件数({record_count})")

    # レコードがあれば
    if record_count:
        # 統計情報集計レコードよりデータフレームを復元する。
        stats_info_collect_data = StatsInfoCollectData()
        for stats_info_collect_record in stats_info_collect_records:
            stats_info_collect_data.dataframe_recovery(stats_info_collect_record)

        return stats_info_collect_data
    else:
        return None
