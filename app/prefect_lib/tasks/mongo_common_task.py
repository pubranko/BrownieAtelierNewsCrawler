import os
from datetime import datetime, date, time
from dateutil.relativedelta import relativedelta
from prefect import task, get_run_logger
from shared.settings import TIMEZONE, DATA_DIR__BACKUP_BASE_DIR
from prefect_lib.flows import START_TIME


@task
def mongo_common_task(
    prefix:str,   # export先のフォルダ名先頭に拡張した名前を付与する。
    suffix:str,   # export先のフォルダ名の末尾に拡張した名前を付与する。
    period_month_from:int,  # 月次エクスポートを行うデータの基準年月
    period_month_to:int,  # 月次エクスポートを行うデータの基準年月
    ) -> tuple[str,datetime, datetime]:
    '''
    mongoDBのコレクションよりimport/exportを行うための前処理を実行する。
    ①import/export先のフォルダ名の生成  ex) prefix_yyyy-mm_yyyy-mm_suffix, prefix_yyyy-mm_yyyy-mm, yyyy-mm_yyyy-mm_suffix, yyyy-mm_yyyy-mm
    ②exportを行う範囲の日時を生成
    '''
    logger = get_run_logger()   # PrefectLogAdapter
    logger.info(f'=== 引数 : prefix={prefix} suffix={suffix} period_month_from~to= {period_month_from} ~ {period_month_to}')


    period_from: datetime = datetime.combine(
        date.today() - relativedelta(months=period_month_from),
        time.min,
        TIMEZONE
        ) + relativedelta(day=1)
    period_to: datetime = datetime.combine(
        date.today() - relativedelta(months=period_month_to),
        time.max,
        TIMEZONE
        ) + relativedelta(day=99)

    pre:str = ''
    if prefix:
        pre = prefix + '_'
    suf:str = ''
    if suffix:
        suf = '_' + suffix

    start_period_yyyy_mm: str = period_from.strftime('%Y-%m')
    end_period_yyyy_mm: str = period_to.strftime('%Y-%m')

    folder_name:str = f'{pre}{start_period_yyyy_mm}_{end_period_yyyy_mm}{suf}'
    dir_path = os.path.join(DATA_DIR__BACKUP_BASE_DIR, folder_name)

    logger.info(f'=== 結果 : {dir_path} {period_from} {period_to}')

    return dir_path, period_from, period_to


'''
保存するフォルダ内の形式

prefix_yyyy-mm_yyyy-mm_suffix
    timestamp
    collection
    collection
    ,,,,
'''