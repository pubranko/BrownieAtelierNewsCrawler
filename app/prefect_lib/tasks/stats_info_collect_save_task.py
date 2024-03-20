from prefect import task, get_run_logger

from prefect_lib.data_models.stats_info_collect_data import StatsInfoCollectData
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.stats_info_collect_model import (
    StatsInfoCollectModel,
)


@task
def stats_info_collect_save_task(
    mongo: MongoModel, stats_info_collect_data: StatsInfoCollectData
):
    """ """
    logger = get_run_logger()  # PrefectLogAdapter

    # 集計結果を保存
    stats_info_collect_model = StatsInfoCollectModel(mongo)

    # レコードタイプ = spider_stats の出力
    stats_info_collect_model.stats_update(
        stats_info_collect_data.spider_df.to_dict(orient="records")
    )

    # レコードタイプ = robots_response_status の出力
    stats_info_collect_model.stats_update(
        stats_info_collect_data.robots_df.to_dict(orient="records"),
        stats_info_collect_data.ROBOTS_RESPONSE_STATUS,
    )

    # レコードタイプ = downloader_response_status の出力
    stats_info_collect_model.stats_update(
        stats_info_collect_data.downloader_df.to_dict(orient="records"),
        stats_info_collect_data.DOWNLOADER_RESPONSE_STATUS,
    )
