# -*- coding:utf-8 -*-

'__author__' == 'weiss'

import json
from hashlib import md5
import os
from json import JSONDecodeError
import pymongo
import re
from urllib.parse import urlencode
import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
from config import *
from multiprocessing import Pool

# 连接mongodb
client = pymongo.MongoClient(MONGO_URL, MONGO_PORT)
db = client[MONGO_DB]

# 爬取Ajax页面时方法，用urlencode()方法解析data，并将data与url拼成完整的url然后进行get请求，
# 访问出索引页的html，并错索引页面解析出想要的目标的子url，进行获取得到目标的详情页，并进行分析

# 获取所要爬取的索引页，并返回get到的内容

def get_page_index(offset, keyword):
    data = {
        'offset': offset,
        'format': 'json',
        'keyword': keyword,
        'autoload': 'true',
        'count': 20,
        'cur_tab': 3,
        'from': 'gallery'
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)'
    }
    url = 'https://www.toutiao.com/search_content/?' + urlencode(data)
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        print('请求索引页出错', url)
        return None

'''
# 解析返回到的索引页内容
#json.loads()函数直接将json数据解析成python字典，data.keys()可以获取所有的键。
#yield 可以将函数变成生成器，后面可以使用for循环依次获取这个函数yield出来的值。
'''
def parse_page_index(html):
    try:
        data = json.loads(html)
        if data and 'data' in data.keys():
            for item in data.get('data'):
                # 利用生成器取出详情页的url
                yield item.get('article_url')
    except JSONDecodeError:
        pass


# 获取详情页的内容，并返回出去
def get_page_detail(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0'
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        print('获取详情页出错', url)
        return None

# 用bs4和正则分别将其title和url解析出去
def parse_page_detail(html, url):
    soup = BeautifulSoup(html, 'lxml')
    title = soup.select('title')[0].get_text()
    print(title)
    images_pattern = re.compile('.*?gallery: JSON.parse(.*?)siblingList', re.S)
    result = re.search(images_pattern, html)
    #print(result)
    if result:
        result_url = str(result.group(0))
        #print(result_url)
        images_url = re.findall(r'url\\":\\"(.*?)\\"', result_url, re.S)
        images = [item.replace('\\', '') for item in images_url]
        # print(images)
        # 用遍历出来的每个图片url传入下载图片方法中
        for image in images: download_images(image)
        return {
                'title': title,
                'url': url,
                'images': images
        }

# 保存到mongodb中
def save_to_mongo(result):
    # 向mongodb的表中插入数据result(就是在详情页面中解析出来的数据，parse_page_detail()方法中return的数据)
    if db[MONGO_TABLE].insert(result):
        print('存储到MongoDB成功', result)
        return True
    return False


# requests请求详情页中解析出的目标url
def download_images(url):
    headers = {
        'User':"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11"
    }
    print('正在下载', url)
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # response.content返回的时二进制流信息(图片，视频), response.text返回的数据为文本格式
            # 将二进制流内容传入到保存图片的方法中
            save_image(response.content)
        return None
    except RequestException:
        print('获取详情页出错', url)
        return None

# os.getcwd()方法时当前目录， md5是为了防止得到的信息是重复的(运用不同的md5信息来分辨)
def save_image(content):
    file_path = '{0}/{1}.{2}'.format(os.getcwd(), md5(content).hexdigest(), 'jpg')
    if not os.path.exists(file_path):
        with open(file_path, 'wb') as f:
            f.write(content)
            f.close()


def main(offset):

    html = get_page_index(offset, KEYWORD)
    for url in parse_page_index(html):
        html = get_page_detail(url)
        if html:
            result = parse_page_detail(html, url)
            if result: save_to_mongo(result)



# 提高爬取速度，运用了进程池
if __name__ == '__main__':

    groups = [x*20 for x in range(GROUP_START, GROUP_END+1)]
    pool = Pool()
    pool.map(main, groups)