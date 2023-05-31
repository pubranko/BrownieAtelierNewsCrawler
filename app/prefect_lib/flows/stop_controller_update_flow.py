from typing import Final
from prefect import flow, task, get_run_logger
from prefect.futures import PrefectFuture
from prefect.task_runners import SequentialTaskRunner
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.end_task import end_task
from prefect_lib.flows.common_flow import common_flow
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel


class StopControllerUpdateConst:
    '''Stop Controller Update Flow用コンスタントクラス'''

    COMMAND_ADD:Final[str] = 'add'
    '''コマンド：追加'''
    COMMAND_DELETE:Final[str] = 'delete'
    '''コマンド：削除'''
    CRAWLING: Final[str] = 'crawling'
    '''停止対象：クローリング'''
    SCRAPYING: Final[str] = 'scrapying'
    '''停止対象：スクレイピング'''

@task
def stop_controller_update_task(domain: str, command: str, destination: str, mongo: MongoModel):
    '''
    クロール対象のドメインの登録・削除を行う。
    スクレイピング対象のドメインの登録・削除を行う。
    '''
    logger = get_run_logger()   # PrefectLogAdapter
    logger.info(f'=== stop_controller_update_task 引数: {str(domain)} / {str(command)} / {str(destination)}')

    controller = ControllerModel(mongo)
    if destination == StopControllerUpdateConst.CRAWLING:
        record: list = controller.crawling_stop_domain_list_get()
    elif destination == StopControllerUpdateConst.SCRAPYING:
        record: list = controller.scrapying_stop_domain_list_get()
    else:
        logger.error(
            f'=== Stop Controller Update Task : destinationパラメータエラー : {destination}')
        raise ValueError(destination)

    logger.info(
        f'=== Stop Controller Update Task  : 更新前の登録状況 : {record}')

    if command == StopControllerUpdateConst.COMMAND_ADD:
        pass
        record.append(domain)
    elif command == StopControllerUpdateConst.COMMAND_DELETE:
        pass
        if domain in record:
            record.remove(domain)
        else:
            logger.error(
                f'=== Stop Controller Update Task  : domainの登録がありません : {domain}')
            raise ValueError(domain)
    else:
        logger.error(
            f'=== Stop Controller Update Task : commandパラメータエラー : {command}')
        raise ValueError(command)

    # domainの重複除去
    _ = list(set(record))

    # 更新した内容でアップデート
    if destination == StopControllerUpdateConst.CRAWLING:
        controller.crawling_stop_domain_list_update(_)
    else:
        controller.scrapying_stop_domain_list_update(_)

    logger.info(
        f'=== Stop Controller Update Task : 更新後の登録状況 : {_}')


@flow(
    flow_run_name='[ENTRY_003] Stop Controller Update Flow',
    task_runner=SequentialTaskRunner())
@common_flow
def stop_controller_update_flow(domain: str, command: str, destination: str):

    # ロガー取得
    logger = get_run_logger()   # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    if init_task_result.get_state().is_completed():
        mongo = init_task_result.result()

        try:
            stop_controller_update_task(domain, command, destination, mongo)
        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f'=== {e}')
        finally:
            # 後続の処理を実行する
            end_task(mongo)
    else:
        logger.error(f'=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。')
