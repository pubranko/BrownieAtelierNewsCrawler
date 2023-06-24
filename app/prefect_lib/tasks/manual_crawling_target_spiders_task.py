from typing import Any
from prefect import task, get_run_logger
from shared.directory_search_spiders import DirectorySearchSpiders

@task
def manual_crawling_target_spiders_task(spider_names: list[str]) -> list[dict[str, Any]]:
    '''
    scrapyによるクロールを実行するための対象スパイダー情報の一覧を生成する。
    '''
    logger = get_run_logger()   # PrefectLogAdapter
    logger.info(f'=== 引数 : spider_names={spider_names}')

    # threads: list[threading.Thread] = []
    directory_search_spiders = DirectorySearchSpiders()

    # 対象スパイダー情報、スパイダー名称保存リスト
    crawling_target_spiders: list[dict[str, Any]] = []
    error_spider_names: list = []

    # spidersディレクトリよりスパイダー名の一覧を取得
    spiders_name_list = directory_search_spiders.spiders_name_list_get()

    # 引数より渡されたスパイダーの一覧が、spidersディレクトリより取得した一覧に存在するかチェック
    args_spiders_name = set(spider_names)
    for args_spider_name in args_spiders_name:
        if not args_spider_name in spiders_name_list:
            error_spider_names.append(args_spider_name)
    # 引数で渡されたスパイダーが存在しなかった場合、エラー情報をログに出力して停止させる
    if len(error_spider_names):
        logger.error(
            f'=== scrapy crwal run : 指定されたspider_nameは存在しませんでした : {error_spider_names}')
        raise ValueError(error_spider_names)

    crawling_target_spiders:list[dict[str, Any]] = directory_search_spiders.spiders_info_list_get(args_spiders_name)

    return  crawling_target_spiders
