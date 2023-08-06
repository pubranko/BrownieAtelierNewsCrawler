
from typing import Final

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
