# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.

#####################
import logging
'''
 不要なhttpxのログを抑制
'''
# logging.getLogger('scrapy.core.engine').setLevel(logging.INFO)    #効果がなかった、、、
logging.getLogger('scrapy.core.scraper').setLevel(logging.INFO)
logging.getLogger('filelock').setLevel(logging.INFO)
# seleniumのロガー抑制
logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)
logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)


