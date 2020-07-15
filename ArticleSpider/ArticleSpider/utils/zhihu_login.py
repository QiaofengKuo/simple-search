import os
import base64
import copy
import hmac
import json
import time
from hashlib import sha1
from urllib import parse

import requests
import execjs
from zheye import zheye
from requests_toolbelt.multipart import MultipartEncoder

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
}
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class ZhiHuLogin(object):
    home_url = "https://www.zhihu.com/signin?next=%2F"
    udid_url = "https://www.zhihu.com/udid"
    sign_in_url = "https://www.zhihu.com/api/v3/oauth/sign_in"
    captcha_url = "https://www.zhihu.com/api/v3/oauth/captcha?lang=cn"

    def __init__(self, username, password):
        self.username = username
        self.password = password

        self.session = self.get_cookies()
        self.captcha = ""

    def get_cookies(self):
        session = requests.session()
        session.get(url=self.home_url, headers=HEADERS)
        session.post(url=self.udid_url, headers=HEADERS)
        return session

    def download_captcha(self, img_path):
        response = self.session.put(url=self.captcha_url, headers=HEADERS)
        img_base64 = response.json().get("img_base64")
        with open(img_path, 'wb') as f:
            f.write(base64.b64decode(img_base64))

    def post_captcha(self, img_path):
        ocr = zheye()
        positions = ocr.Recognize(img_path)
        input_points = [[i[1] / 2, i[0] / 2] for i in positions]
        input_text = {"img_size": [200, 44], "input_points": input_points}
        payload = MultipartEncoder(
            fields={"input_text": json.dumps(input_text)}
        )
        headers = copy.deepcopy(HEADERS)
        headers.update({
            "content-type": payload.content_type
        })
        response = self.session.post(url=self.captcha_url, data=payload, headers=headers)
        return response, input_text

    def check_captcha(self):
        response = self.session.get(url=self.captcha_url, headers=HEADERS)
        result = response.json()
        if result.get("show_captcha"):
            print("需要验证码, 开始识别.....")
            img_path = os.path.join(BASE_DIR, "captcha.jpg")
            self.download_captcha(img_path)
            response, point = self.post_captcha(img_path)
            submit_captcha_success = response.json().get("success")
            if submit_captcha_success:
                print("验证码识别成功")
                self.captcha = str(point)
            else:
                print("验证码识别失败")
                self.check_captcha()
        else:
            print("不需要验证码")

    @staticmethod
    def get_signature(timestamp):
        a = hmac.new(b'd1b964811afb40118a12068ff74a12f4', digestmod=sha1)
        a.update(b'password')
        a.update(b'c3cef7c66a1843f8b3a9e6a1e3160e20')
        a.update(b'com.zhihu.web')
        a.update(bytes(str(timestamp), encoding="utf-8"))
        signature = a.hexdigest()
        return signature

    @staticmethod
    def exec_js_function(js_file_path, func, *params):
        with open(js_file_path, 'r') as f:
            lines = f.readlines()
            js = "".join(lines)
            js_context = execjs.compile(js)
            result = js_context.call(func, *params)
            return result

    def login(self):
        self.check_captcha()
        timestamp = int(time.time() * 1000)
        signature = self.get_signature(timestamp)
        data = {
            "client_id": "c3cef7c66a1843f8b3a9e6a1e3160e20",
            "grant_type": "password",
            "timestamp": timestamp,
            "source": "com.zhihu.web",
            "signature": signature,
            "username": self.username,
            "password": self.password,
            "captcha": self.captcha,
            "lang": "cn",
            "utm_source": "",
            "ref_source": "other_https://www.zhihu.com/signin?next=%2F"
        }
        HEADERS.update({
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://www.zhihu.com",
            "x-zse-83": "3_2.0"
        })
        payload = parse.urlencode(data)
        js_file_path = os.path.join(BASE_DIR, "encrypt.js")
        encrypt_data = self.exec_js_function(js_file_path, 'b', payload)
        response = self.session.post(url=self.sign_in_url, data=encrypt_data, headers=HEADERS)
        if response.status_code == 201:
            # res = self.session.get(url="https://www.zhihu.com", headers=HEADERS)
            # with open("index_page.html", "wb") as fp:
            #     fp.write(res.text.encode("utf-8"))
            print("模拟登录成功o(*￣▽￣*)ブ")
            return requests.utils.dict_from_cookiejar(response.cookies)
        else:
            return {}


if __name__ == "__main__":
    login_test = ZhiHuLogin("账号", "密码")
    cookies = login_test.login()
    print(cookies)
