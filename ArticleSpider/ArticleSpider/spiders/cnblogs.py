import json
import re
from urllib import parse

import scrapy
from scrapy import Request
import requests

from ArticleSpider.utils import common
from ArticleSpider.items import ArticleItemLoader, CnBlogsArticleItem


class CnblogsSpider(scrapy.Spider):
    name = 'cnblogs'
    allowed_domains = ['news.cnblogs.com']
    start_urls = ['http://news.cnblogs.com/']

    def parse(self, response):
        post_nodes = response.css('#news_list .news_block')
        for post_node in post_nodes:
            image_url = post_node.css('.entry_summary a img::attr(src)').extract_first("")
            post_url = post_node.css('h2 a::attr(href)').extract_first("")
            yield Request(url=parse.urljoin(response.url, post_url),
                          meta={"front_image_url": image_url},
                          callback=self.parse_detail)

        # 提取下一页
        # Xpath选择器
        next_url = response.xpath("//a[contains(text(), 'Next >')]/@href").extract_first("")
        yield Request(url=parse.urljoin(response.url, next_url), callback=self.parse)

        #  CSS选择器
        # next_page = response.css("div.pager a:last-child::text").extract_first("")
        # if next_page == "Next >":
        #     next_url = response.css("div.pager a:list-child::attr(href)").extract_first("")
        #     yield Request(url=parse.urljoin(response.url, next_url), callback=self.parse)

    def parse_detail(self, response):
        match_id = re.match(r'.*?(\d+)', response.url)
        if match_id:
            post_id = match_id.group(1)
            front_image_url = response.meta.get("front_image_url", "")
            # article_item = CnBlogsArticleItem()
            # title = response.css("#news_title a::text").extract_first("")
            # create_date = response.css("#news_info .time::text").extract_first("")
            # match_date = re.match(r".*?(\d+.*)", create_date)
            # if match_date:
            #     create_date = match_date.group(1)
            # content = response.css("#news_content").extract()[0]
            # tag_list = response.css(".news_tags a::text").extract()
            # tags = ",".join(tag_list)
            #
            # article_item['title'] = title
            # article_item['create_date'] = create_date
            # article_item['content'] = content
            # article_item['tags'] = tags
            # article_item['url'] = response.url
            # if front_image_url:
            #      article_item['front_image_url'] = [parse.urljoin('https://', front_image_url)]
            # else:
            #     article_item['front_image_url'] = []

            # 使用ItemLoader方式对数据收集
            item_loader = ArticleItemLoader(item=CnBlogsArticleItem(), response=response)
            item_loader.add_css("title", "#news_title a::text")
            item_loader.add_css("create_date", "#news_info .time::text")
            item_loader.add_css("content", "#news_content")
            item_loader.add_css("tags", ".news_tags a::text")
            item_loader.add_value("url", response.url)
            item_loader.add_value("url_object_id", common.get_md5(response.url))
            if front_image_url:
                item_loader.add_value("front_image_url", parse.urljoin('https://', front_image_url))

            yield Request(url=parse.urljoin(response.url, "/NewsAjax/GetAjaxNewsInfo?contentId={}".format(post_id)),
                          meta={"article_item": item_loader},
                          callback=self.parse_nums)

            # 同步的方式请求数据，较简单
            # html = requests.get(parse.urljoin(response.url, "/NewsAjax/GetAjaxNewsInfo?contentId={}".format(post_id)))
            # json_data = json.loads(html.text)
            # praise_nums = json_data["DiggCount"]
            # fav_nums = json_data["TotalView"]
            # comment_nums = json_data["CommentCount"]

    def parse_nums(self, response):
        json_data = json.loads(response.text)
        item_loader = response.meta.get("article_item", "")
        item_loader.add_value("praise_nums", json_data["DiggCount"])
        item_loader.add_value("fav_nums", json_data["TotalView"])
        item_loader.add_value("comment_nums", json_data["CommentCount"])

        # praise_nums = json_data["DiggCount"]
        # fav_nums = json_data["TotalView"]
        # comment_nums = json_data["CommentCount"]

        # article_item["praise_nums"] = praise_nums
        # article_item["fav_nums"] = fav_nums
        # article_item["comment_nums"] = comment_nums
        # article_item["url_object_id"] = common.get_md5(article_item['url'])

        article_item = item_loader.load_item()
        yield article_item
