import os
import re
from typing import Any, Union
from datetime import datetime
from logging import Logger, StreamHandler, LoggerAdapter
from prefect.logging.loggers import PrefectLogAdapter
from prefect import task
from prefect import get_run_logger
from prefect.context import FlowRunContext
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierNotice.mail_send import mail_send
from shared.resource_check import resource_check
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel


'''
mongoDBのインポートを行う。
・pythonのlistをpickle.loadsで復元しインポートする。
・対象のコレクションを選択できる。
・対象の年月を指定できる。範囲を指定した場合、月ごとにエクスポートを行う。
'''


@task
# def end_task(start_time: datetime, log_file_path: str, mongo: MongoModel, flow_context:FlowRunContext):
def end_task(start_time: datetime, log_file_path: str, mongo: MongoModel, flow_name:str):
    '''Flow共通終了処理'''

    def log_check(log_record:str, logger:Union[Logger,LoggerAdapter]):
        '''クリティカル、エラー、ワーニングがあったらメールで通知'''

        #CRITICAL > ERROR > WARNING > INFO > DEBUG
        # 2021-08-08 12:31:04 [scrapy.core.engine] INFO: Spider closed (finished)
        # クリティカルの場合、ログ形式とは限らない。raiseなどは別形式のため、後日検討要。
        pattern_traceback = re.compile(r'Traceback.*:')
        pattern_critical = re.compile(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} CRITICAL ')
        pattern_error = re.compile(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} ERROR ')
        # pattern_warning = re.compile(
        #     r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} WARNING ')
        # 2021-08-08 12:31:04 INFO [prefect.FlowRunner] : Flow run SUCCESS: all reference tasks succeeded

        title: str = ''
        if pattern_traceback.search(log_record):
            # title = f'【{flow_context.flow_run.name}:クリティカル発生】{start_time.isoformat()}'
            title = f'【{flow_name}:クリティカル発生】{start_time.isoformat()}'
        elif pattern_critical.search(log_record):
            # title = f'【{flow_context.flow_run.name}:クリティカル発生】{start_time.isoformat()}'
            title = f'【{flow_name}:クリティカル発生】{start_time.isoformat()}'
        elif pattern_error.search(log_record):
            # title = f'【{flow_context.flow_run.name}:エラー発生】{start_time.isoformat()}'
            title = f'【{flow_name}:エラー発生】{start_time.isoformat()}'
        # elif pattern_warning.search(self.log_record):
        #     title = f'【{self.name}:ワーニング発生】{self.start_time.isoformat()}'

        if title:
            msg: str = '\n'.join([
                '【ログ】', log_record,
            ])
            mail_send(title, msg, logger)

    def log_save(log_record:str):
        '''処理が終わったらログを保存'''
        crawler_logs = CrawlerLogsModel(mongo)
        crawler_logs.insert_one({
            CrawlerLogsModel.START_TIME: start_time,
            CrawlerLogsModel.FLOW_NAME: flow_name,
            CrawlerLogsModel.RECORD_TYPE: CrawlerLogsModel.RECORD_TYPE__FLOW_REPORTS,
            CrawlerLogsModel.LOGS: log_record,
        })

    # ロガー取得
    logger = get_run_logger()   # PrefectLogAdapter
    logger.info(f'=== end_task開始:  {start_time}, {log_file_path}, {flow_name}')
    resource_check(logger)

    # logファイルを確認しエラーの有無をチェックする。
    with open(log_file_path) as f:
        log_record = f.read()
    log_check(log_record, logger)

    # logをmongoDBへ保存しクローズ処理を実施。
    log_save(log_record)
    mongo.close()

    # 不要となったログファイルを削除
    if log_file_path:
        os.remove(log_file_path)
