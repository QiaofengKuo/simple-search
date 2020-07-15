import re
import json
import datetime
from urllib import parse

import scrapy
from scrapy.loader import ItemLoader
from ArticleSpider.items import ZhiHuQuestionItem, ZhiHuAnswerItem

from ArticleSpider.utils.zhihu_login import ZhiHuLogin


class ZhihuSpider(scrapy.Spider):
    name = 'zhihu'
    allowed_domains = ['www.zhihu.com']
    start_urls = ['https://www.zhihu.com/']
    headers = {
        "HOST": "www.zhihu.com",
        "Referer": "https://www.zhihu.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)\
                      Chrome/83.0.4103.116 Safari/537.36"
    }
    start_answer_url = "https://www.zhihu.com/api/v4/questions/{0}/answers?include=data%5B*%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%2Cis_recognized%2Cpaid_info%2Cpaid_info_content%3Bdata%5B*%5D.mark_infos%5B*%5D.url%3Bdata%5B*%5D.author.follower_count%2Cbadge%5B*%5D.topics"
    def start_requests(self):
        cookies = ZhiHuLogin("账号", "密码").login()
        yield scrapy.Request(url=self.start_urls[0], cookies=cookies, dont_filter=True)

    def parse(self, response):
        all_urls = response.css("a::attr(href)").extract()
        for url in all_urls:
            print(url)
            match_obj = re.match(r"(.*zhihu.com/question/(\d+))(/|$).*", url)
            if match_obj:
                request_url = parse.urljoin("https://", match_obj.group(1))
                question_id = match_obj.group(2)
                print(request_url, question_id)
                yield scrapy.Request(url=request_url, headers=self.headers,
                                     meta={"question_id": question_id}, callback=self.parse_question)

    def parse_question(self, response):
        question_id = int(response.meta.get("question_id", "1"))
        item_loader = ItemLoader(item=ZhiHuQuestionItem(), response=response)
        item_loader.add_css("title", "h1.QuestionHeader-title::text")
        item_loader.add_css("content", ".QuestionHeader-detail")
        item_loader.add_value("url", response.url)
        item_loader.add_value("question_id", question_id)
        item_loader.add_css("answer_num", ".List-headerText span::text")
        item_loader.add_css("comments_num", ".QuestionHeader-Comment button::text")
        item_loader.add_css("watch_user_num", ".NumberBoard-itemValue::text")
        item_loader.add_css("topics", ".QuestionHeader-topics .Popover div::text")

        question_item = item_loader.load_item()

        yield scrapy.Request(url=self.start_answer_url.format(question_id), headers=self.headers,
                             callback=self.parse_answer)
        yield question_item

    def parse_answer(self, response):
        ans_json = json.loads(response.text)
        is_end = ans_json["paging"]["is_end"]
        next_url = ans_json["paging"]["next"]

        for answer in ans_json["data"]:
            answer_item = ZhiHuAnswerItem()
            answer_item["answer_id"] = answer["id"]
            answer_item["url"] = answer["url"]
            answer_item["question_id"] = answer["question"]["id"]
            answer_item["author_id"] = answer["author"]["id"] if "id" in answer["author"] else None
            answer_item["content"] = answer["content"] if "content" in answer else None
            answer_item["praise_num"] = answer["voteup_count"]
            answer_item["comments_num"] = answer["comment_count"]
            answer_item["create_time"] = answer["created_time"]
            answer_item["update_time"] = answer["updated_time"]
            answer_item["crawl_time"] = datetime.datetime.now()

            yield answer_item

        if not is_end:
            yield scrapy.Request(next_url, headers=self.headers, callback=self.parse_answer)
