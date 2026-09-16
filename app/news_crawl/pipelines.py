# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface


from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider


class MongoPipeline:
    def __init__(self):
        pass

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider: ExtensionsCrawlSpider | ExtensionsSitemapSpider):
        crawler_response = CrawlerResponseModel(spider.mongo)
        crawler_response.insert_one(dict(item))

        return item
