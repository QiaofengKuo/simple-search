import codecs
import json

from scrapy.exporters import JsonItemExporter
from scrapy.pipelines.images import ImagesPipeline
from twisted.enterprise import adbapi

import MySQLdb

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class ArticlespiderPipeline(object):
    def process_item(self, item, spider):
        return item


# 同步入库
class MysqlPipeline(object):
    def __init__(self):
        self.conn = MySQLdb.connect("127.0.0.1", 'root', '123456', 'article_spider', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """
            insert into cnblogs_article(title, url, url_object_id, front_image_url, front_image_path, praise_nums, comment_nums, fav_nums, tags, content, create_date)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE praise_nums=VALUES(praise_nums)
        """
        params = list()
        params.append(item.get("title", ""))
        params.append(item.get("url", ""))
        params.append(item.get("url_object_id", ""))
        front_image = ",".join(item.get("front_image_url", []))
        params.append(front_image)
        params.append(item.get("front_image_path",""))
        params.append(item.get("praise_nums", 0))
        params.append(item.get("comment_nums", 0))
        params.append(item.get("fav_nums", 0))
        params.append(item.get("tags", ""))
        params.append(item.get("content", ""))
        params.append(item.get("create_date", "2020-06-01"))
        self.cursor.execute(insert_sql, tuple(params))
        self.conn.commit()
        return item


# 异步入库
class MysqlTwistedPipeline(object):
    def __init__(self, db_pool):
        self.db_pool = db_pool

    @classmethod
    def from_settings(cls, settings):
        from MySQLdb.cursors import DictCursor
        db_params = dict(
            host=settings['MYSQL_HOST'],
            db=settings["MYSQL_DBNAME"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
            charset='utf8',
            cursorclass=DictCursor,
            use_unicode=True
        )
        db_pool = adbapi.ConnectionPool("MySQLdb", **db_params)
        return cls(db_pool)

    def process_item(self, item, spider):
        query = self.db_pool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider)
        return item

    def handle_error(self, failure, item, spider):
        print(failure)

    def do_insert(self, cursor, item):
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, tuple(params))


# Json方式一：自定义
class JsonWithEncodingPipeline(object):
    def __init__(self):
        self.file = codecs.open("article.json", "w", encoding="utf-8")

    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(lines)
        return item

    def spider_close(self, spider):
        self.file.close()


# Json方式二：使用自带的exporters
class JsonExporterPipeline(object):
    def __init__(self):
        self.file = open("articleExport.json", "wb")
        self.exporter = JsonItemExporter(self.file, encoding="utf-8", ensure_ascii=False)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_close(self, spider):
        self.exporter.finish_exporting()
        self.file.close()


class ArticleImagesPipeline(ImagesPipeline):
    def item_completed(self, results, item, info):

        if "front_image_url" in item:
            image_file_path = ""
            for ok, value in results:
                image_file_path = value['path']
            item["front_image_path"] = image_file_path

        return item


class ElasticsearchPipeline(object):
    # 将数据写入到es中

    def process_item(self, item, spider):
        # 将item转换为es的数据
        item.save_to_es()
        return item
