# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
import re
import datetime

from w3lib.html import remove_tags

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Identity, Join

from elasticsearch_dsl.connections import connections
es = connections.create_connection(ArticleType._doc_type.using)
import redis
redis_cli = redis.StrictRedis()

from ArticleSpider.settings import SQL_DATETIME_FORMAT, SQL_DATE_FORMAT
from ArticleSpider.utils.common import extract_num
from ArticleSpider.models.es import ArticleType


class ArticlespiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class ArticleItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


def date_convert(value):
    match_date = re.match(r".*?(\d+.*)", value)
    if match_date:
        return match_date.group(1)
    else:
        return "0000-00-00"


def gen_suggests(index, info_tuple):
    # 根据字符串生成搜索建议数组
    used_words = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            # 调用es的analyze接口分析字符串
            words = es.indices.analyze(index=index, analyzer="ik_max_word", params={'filter':["lowercase"]}, body=text)
            analyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"])>1])
            new_words = analyzed_words - used_words
            used_words.update(analyzed_words)
        else:
            new_words = set()

        if new_words:
            suggests.append({"input": list(new_words), "weight": weight})

    return suggests


class CnBlogsArticleItem(scrapy.Item):
    title = scrapy.Field()
    create_date = scrapy.Field(
        input_processor=MapCompose(date_convert)
    )
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field(
        output_processor=Identity()
    )
    front_image_path = scrapy.Field()
    praise_nums = scrapy.Field()
    fav_nums = scrapy.Field()
    comment_nums = scrapy.Field()
    tags = scrapy.Field(
        output_processor=Join(separator=",")
    )
    content = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into cnblogs_article
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE content=VALUES(fav_nums)
        """

        front_image = ",".join(self.get("front_image_url", []))
        params = (
            self.get("title", ""),
            self.get("url", ""),
            self.get("url_object_id", ""),
            front_image,
            self.get("front_image_path", ""),
            self.get("praise_nums", 0),
            self.get("comment_nums", 0),
            self.get("fav_nums", 0),
            self.get("tags", ""),
            self.get("content", ""),
            self.get("create_date", "0000-00-00"),
        )

        return insert_sql, params

    def save_to_es(self):
        article = ArticleType()
        article.title = self['title']
        article.create_date = self["create_date"]
        article.content = remove_tags(self["content"])
        article.front_image_url = self["front_image_url"]
        if "front_image_path" in self:
            article.front_image_path = self["front_image_path"]
        article.praise_nums = self["praise_nums"]
        article.fav_nums = self["fav_nums"]
        article.comment_nums = self["comment_nums"]
        article.url = self["url"]
        article.tags = self["tags"]
        article.meta.id = self["url_object_id"]

        article.suggest = gen_suggests(ArticleType._doc_type.index, ((article.title, 10), (article.tags, 7)))

        article.save()

        redis_cli.incr("cnblogs_count")

        return


class ZhiHuQuestionItem(scrapy.Item):
    question_id = scrapy.Field()
    topics = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    answer_num = scrapy.Field()
    comments_num = scrapy.Field()
    watch_user_num = scrapy.Field()
    click_num = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        # 插入知乎question表的sql语句
        insert_sql = """
            insert into zhihu_question(question_id, topics, url, title, content, answer_num, comments_num,
              watch_user_num, click_num, crawl_time
              )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE content=VALUES(content), answer_num=VALUES(answer_num), comments_num=VALUES(comments_num),
              watch_user_num=VALUES(watch_user_num), click_num=VALUES(click_num)
        """
        question_id = self["question_id"][0]
        topics = ",".join(self["topics"])
        url = self["url"][0]
        title = "".join(self["title"])
        content = "".join(self["content"])
        answer_num = extract_num("".join(self["answer_num"]))
        comments_num = extract_num("".join(self["comments_num"]))

        if len(self["watch_user_num"]) == 2:
            watch_user_num = self["watch_user_num"][0]
            click_num = self["watch_user_num"][1]
        else:
            watch_user_num = self["watch_user_num"][0]
            click_num = "0"

        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)

        params = (question_id, topics, url, title, content, answer_num, comments_num,
                  watch_user_num, click_num, crawl_time)

        return insert_sql, params


class ZhiHuAnswerItem(scrapy.Item):
    answer_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    praise_num = scrapy.Field()
    comments_num = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        # 插入知乎answer表的sql语句
        insert_sql = """
            insert into zhihu_answer(answer_id, url, question_id, author_id, content, praise_num, comments_num,
              create_time, update_time, crawl_time
              ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              ON DUPLICATE KEY UPDATE content=VALUES(content), comments_num=VALUES(comments_num), 
              praise_num=VALUES(praise_num), update_time=VALUES(update_time)
        """

        create_time = datetime.datetime.fromtimestamp(self["create_time"]).strftime(SQL_DATETIME_FORMAT)
        update_time = datetime.datetime.fromtimestamp(self["update_time"]).strftime(SQL_DATETIME_FORMAT)
        params = (
            self["answer_id"],
            self["url"],
            self["question_id"],
            self["author_id"],
            self["content"],
            self["praise_num"],
            self["comments_num"],
            create_time, update_time,
            self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
        )

        return insert_sql, params


def remove_splash(value):
    # 去掉工作城市的斜线
    return value.replace("/", "")


def handle_job_addr(value):
    addr_list = value.split("\n")
    addr_list = [item.strip() for item in addr_list if item.strip() != "查看地图"]
    return "".join(addr_list)


class LaGouJobItemLoader(ItemLoader):
    # 自定义ItemLoader
    default_output_processor = TakeFirst()


class LaGouJobItem(scrapy.Item):
    title = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    salary = scrapy.Field()
    job_city = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    work_years = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    degree_need = scrapy.Field(
        input_processor=MapCompose(remove_splash),
    )
    job_type = scrapy.Field()
    publish_time = scrapy.Field()
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field()
    job_addr = scrapy.Field(
        input_processor=MapCompose(remove_tags, handle_job_addr),
    )
    company_name = scrapy.Field()
    company_url = scrapy.Field()
    tags = scrapy.Field(
        input_processor=Join(",")
    )
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into lagou_job(title, url, url_object_id, salary, job_city, work_years, degree_need,
            job_type, publish_time, job_advantage, job_desc, job_addr, company_name, company_url,
            tags, crawl_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc)
        """
        params = (
            self.get("title", ""),
            self.get("url", ""),
            self.get("url_object_id", ""),
            self.get("salary", ""),
            self.get("job_city", ""),
            self.get("work_years", ""),
            self.get("degree_need", ""),
            self.get("job_type", ""),
            self.get("publish_time", "0000-00-00"),
            self.get("job_advantage", ""),
            self.get("job_desc", ""),
            self.get("job_addr", ""),
            self.get("company_name", ""),
            self.get("company_url", ""),
            self.get("tags", ""),
            self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
        )

        return insert_sql, params

