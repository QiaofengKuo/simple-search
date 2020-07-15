import os
import sys

from scrapy.cmdline import execute

# 添加项目的根目录
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# execute(["scrapy", "crawl", "cnblogs"])
# execute(["scrapy", "crawl", "zhihu"])
execute(["scrapy", "crawl", "lagou"])
