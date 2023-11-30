from prefect import task, get_run_logger
from prefect_lib.data_models.scraper_pattern_report_data import ScraperPatternReportData
from prefect_lib.data_models.scraper_pattern_report_input import ScraperPatternReportInput
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.news_clip_master_model import NewsClipMasterModel
from BrownieAtelierMongo.collection_models.scraper_info_by_domain_model import ScraperInfoByDomainModel


@task
def scraper_pattern_report_data_frame_task(
    mongo: MongoModel,
    scraper_pattern_report_input: ScraperPatternReportInput
    ) -> ScraperPatternReportData:
    '''
    ・ドメイン別にニュースクリップマスターより対象期間のレコード件数を確認。
    ・上記で取得したレコードを元に統計用のデータフレームを生成。
    ・戻り値: スクレイパー情報解析用のデータフレーム管理クラス
    '''
    logger = get_run_logger()   # PrefectLogAdapter
    logger.info(f'=== 引数 : {scraper_pattern_report_input.base_date_get()}')

    # 各mongoコレクションの準備
    news_clip_master = NewsClipMasterModel(mongo)
    scraper_info_by_domain = ScraperInfoByDomainModel(mongo)
    # スクレイパーパターンレポートのデータフレーム準備
    scraper_pattern_report_data = ScraperPatternReportData()
    # 引数より計算した対象期間情報from/toを取得
    base_date_from, base_date_to = scraper_pattern_report_input.base_date_get()

    # ドメイン別にニュースクリップマスターより取得した件数を確認。
    for scraper_info_by_domain_data in scraper_info_by_domain.find_and_data_models_get():

        # ニュースクリップマスターより、対象期間に取得したレコード件数を確認
        conditions: list = []
        conditions.append(
            {NewsClipMasterModel.CRAWLING_START_TIME: {'$gte': base_date_from}})
        conditions.append({NewsClipMasterModel.CRAWLING_START_TIME: {'$lt': base_date_to}})
        conditions.append(
            {NewsClipMasterModel.DOMAIN: scraper_info_by_domain_data.domain_get()})
        filter: dict = {'$and': conditions}
        record_count = news_clip_master.count(filter=filter)

        #
        logger.info(
            f'=== news_clip_master : domain = {scraper_info_by_domain_data.domain_get()},  件数 = {str(record_count)}')

        # データフレーム（マスター）を作成
        skeleton_for_counters: list = scraper_info_by_domain_data.making_into_a_table_format()
        for skeleton_for_counter in skeleton_for_counters:
            scraper_pattern_report_data.scraper_info_master_store(
                skeleton_for_counter)

        # news_clip_masterからレコードを順に取得する。
        # 取得したレコードをデータフレーム（カウント用）へ挿入
        for master_record in news_clip_master.limited_find(
                filter=filter, projection={
                    NewsClipMasterModel.DOMAIN: 1,
                    NewsClipMasterModel.CRAWLING_START_TIME: 1,
                    NewsClipMasterModel.PATTERN: 1}):

            pattern_info: dict = master_record[NewsClipMasterModel.PATTERN]

            for pattern_key, pattern_value in pattern_info.items():
                scraper_pattern_report_data.scraper_info_counter_store({
                    scraper_pattern_report_data.DOMAIN: master_record[NewsClipMasterModel.DOMAIN],
                    scraper_pattern_report_data.SCRAPE_ITEMS: pattern_key,
                    scraper_pattern_report_data.PATTERN: pattern_value,
                    scraper_pattern_report_data.COUNT_OF_USE: 1,
                })

    # 集計対象のnews_clip_masterが1件もない場合処理を中止する。
    if len(scraper_pattern_report_data.scraper_pattern_counter_df) == 0:
        logger.warning(
            f'=== レポート対象のレコードがないため処理をスキップします。')
        raise ValueError

    # データフレーム（カウント用）とデータフレーム（マスター）を使用し、
    # データフレーム（結果）を生成する。
    scraper_pattern_report_data.scraper_info_analysis()

    return scraper_pattern_report_data
