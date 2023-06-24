from prefect import task, get_run_logger
from shared.directory_search_spiders import DirectorySearchSpiders
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from prefect_lib.flows.regular_observation_controller_update_const import RegularObservationControllerUpdateConst
from shared.directory_search_spiders import DirectorySearchSpiders
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel


@task
def regular_observation_controller_update_task(mongo, spiders_name: str, register: str):
    '''
    scrapyによるクロールを実行するための対象スパイダー情報の一覧を生成する。
    '''
    logger = get_run_logger()   # PrefectLogAdapter
    logger.info(f'=== 引数 : {str(spiders_name)} / {str(register)}')
    controller = ControllerModel(mongo)
    record = set(controller.regular_observation_spider_name_set_get())
    logger.info(
        f'=== 更新前の登録内容 : {str(record)}')

    # 引数のスパイダー情報リストをセットへ変換（重複削除）
    spiders_name_set = set(spiders_name)

    # 存在するスパイダーのリスト生成
    directory_search_spiders = DirectorySearchSpiders()
    spiders_exist_set:set = set()
    for spider_info in directory_search_spiders.spiders_name_list_get():
        spiders_exist_set.add(spider_info)

    if register == RegularObservationControllerUpdateConst.REGISTER_ADD:
        for spider_name in spiders_name_set:
            if not spider_name in spiders_exist_set:
                logger.error(
                    f'=== spider_nameパラメータエラー : {spider_name} は存在しません。')
                raise ValueError(spider_name)
        record.update(spiders_name_set)
    elif register == RegularObservationControllerUpdateConst.REGISTER_DELETE:
        for spider_name in spiders_name_set:
            if spider_name in record:
                record.remove(spider_name)
            else:
                logger.error(
                    f'=== spider_nameパラメータエラー : {spider_name} は登録されていません。')
                raise ValueError(spider_name)
    else:
        logger.error(
            f'=== 登録方法(register)パラメータエラー : {register}')
        raise ValueError(register)

    logger.info(
        f'=== 更新後の登録内容 : {str(record)}')

    controller.regular_observation_spider_name_set_update(record)
