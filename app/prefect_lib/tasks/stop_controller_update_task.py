from prefect import task, get_run_logger
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from prefect_lib.flows.stop_controller_update_const import StopControllerUpdateConst
from prefect_lib.data_models.stop_controller_update_input import (
    StopControllerUpdateInput,
)


@task
def stop_controller_update_task(
    stop_controller_update_input: StopControllerUpdateInput, mongo: MongoModel
):
    """
    クロール対象のドメインの登録・削除を行う。
    スクレイピング対象のドメインの登録・削除を行う。
    """
    logger = get_run_logger()  # PrefectLogAdapter

    domain: str = stop_controller_update_input.domain
    command: str = stop_controller_update_input.command
    destination: str = stop_controller_update_input.destination

    logger.info(
        f"=== stop_controller_update_task 引数: {str(domain)} / {str(command)} / {str(destination)}"
    )

    record: list = []
    controller = ControllerModel(mongo)
    if destination == StopControllerUpdateConst.CRAWLING:
        record: list = controller.crawling_stop_domain_list_get()
    elif destination == StopControllerUpdateConst.SCRAPYING:
        record: list = controller.scrapying_stop_domain_list_get()

    logger.info(f"=== Stop Controller Update Task  : 更新前の登録状況 : {record}")

    if command == StopControllerUpdateConst.COMMAND_ADD:
        record.append(domain)
    elif command == StopControllerUpdateConst.COMMAND_DELETE:
        if domain in record:
            record.remove(domain)
        else:
            logger.error(
                f"=== Stop Controller Update Task  : domainの登録がありません : {domain}"
            )
            raise ValueError(domain)

    # domainの重複除去
    _ = list(set(record))

    # 更新した内容でアップデート
    if destination == StopControllerUpdateConst.CRAWLING:
        controller.crawling_stop_domain_list_update(_)
    else:
        controller.scrapying_stop_domain_list_update(_)

    logger.info(f"=== Stop Controller Update Task : 更新後の登録状況 : {_}")
