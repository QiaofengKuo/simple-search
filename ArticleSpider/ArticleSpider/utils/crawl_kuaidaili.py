import parsel

import requests
import MySQLdb

conn = MySQLdb.connect("127.0.0.1", 'root', '123456', 'article_spider', charset="utf8", use_unicode=True)
cursor = conn.cursor()


def crawl_ip():
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)\
         Chrome/83.0.4103.116 Safari/537.36"
    }
    for i in range(1, 3530):
        base_url = "https://www.kuaidaili.com/free/inha/{}/".format(i)
        response = requests.get(base_url, headers=headers)
        html = parsel.Selector(response.text)

        tr_list = html.xpath('//*[@id="list"]/table/tbody/tr')

        for tr in tr_list:
            ip_addr = tr.xpath('./td[1]/text()').extract_first()
            ip_port = tr.xpath('./td[2]/text()').extract_first()
            http_type = tr.xpath('./td[4]/text()').extract_first()
            print(http_type, ip_addr, ip_port)
            cursor.execute(
                "insert proxy_ip(ip, port, proxy_type) VALUES('{0}', '{1}', '{2}')\
                 ON DUPLICATE KEY UPDATE port=VALUES(port)".format(ip_addr, ip_port, http_type)
            )
            conn.commit()


class GetIP(object):

    def delete_ip(self, ip):
        delete_sql = """
            delete from proxy_ip where ip='{0}'
        """.format(ip)
        cursor.execute(delete_sql)
        conn.commit()
        return True

    def judge_ip(self, ip, port):
        http_url = "http://www.baidu.com"
        proxy_url = "http://{0}:{1}".format(ip, port)
        try:
            proxy_dict = {
                "http": proxy_url,
            }
            response = requests.get(http_url, proxies=proxy_dict)
        except Exception as e:
            print("invalid ip and port")
            self.delete_ip(ip)
            return False
        else:
            code = response.status_code
            if code >= 200 and code < 300:
                print("effective ip")
                return True
            else:
                print("invalid ip and port")
                self.delete_ip(ip)
                return False

    def get_random_ip(self):
        random_sql = """
                SELECT ip, port FROM proxy_ip
                ORDER BY RAND()
                LIMIT 1
            """
        result = cursor.execute(random_sql)
        for ip_info in cursor.fetchall():
            ip = ip_info[0]
            port = ip_info[1]

            judge_re = self.judge_ip(ip, port)
            if judge_re:
                return "http://{0}:{1}".format(ip, port)
            else:
                return self.get_random_ip()


if __name__ == "__main__":
    get_ip = GetIP()
    get_ip.get_random_ip()
