# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo


class MongoPipeline(object):
    def __init__(self, mongo_uri, mongo_db, port):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.port = port

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE'),
            port=crawler.settings.get('PORT')

        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri, self.port)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db['user_msg'].update({'url_token': item['url_token']}, {'$set': item}, True)
        return item
