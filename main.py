import random
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lxml import etree
from sqlalchemy import create_engine
import mysql.connector

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/101.0.4951.54 Safari/537.36 Edg/101.0.1210.39 "
}

page_url = []


def one_page(url):
    resp = requests.get(url, headers=headers)
    resp.encoding = "utf-8"
    html = etree.HTML(resp.text)
    lis = html.xpath("/html/body/div[3]/div[1]/div/div[1]/ol/li")

    for li in lis:
        page = li.xpath("./div/div[2]/div[1]/a/@href")[0]
        page_url.append(page)
    resp.close()


def child(href):
    time.sleep(random.randint(3, 5))
    resp = requests.get(href, headers=headers)
    resp.encoding = 'utf-8'
    child_page = BeautifulSoup(resp.text, 'html.parser')
    # 排名
    rank = child_page.find(attrs={'class': 'top250-no'}).text.split('.')[1]
    # 电影名
    film_name = child_page.find(attrs={'property': 'v:itemreviewed'}).text.split(' ')[0]
    # 导演
    director = child_page.find(attrs={'id': 'info'}).text.split('\n')[1].split(':')[1].split('/')
    # 编剧
    scriptwriter = child_page.find(attrs={'id': 'info'}).text.split('\n')[2].split(':')[1].split('/')
    # 主演
    actor = child_page.find(attrs={'id': 'info'}).text.split('\n')[3].split(':')[1].split('/')
    # 类型
    filmType = child_page.find(attrs={'id': 'info'}).text.split('\n')[4].split(':')[1].split('/')

    if child_page.find(attrs={'id': 'info'}).text.split('\n')[5].split(':')[0] == '官方网站':
        # 制片国家/地区
        area = child_page.find(attrs={'id': 'info'}).text.split('\n')[6].split(':')[1].split('/')
        # 语言
        language = child_page.find(attrs={'id': 'info'}).text.split('\n')[7].split(':')[1].split('/')
        # 上映日期
        initialReleaseDate = \
            min(child_page.find(attrs={'id': 'info'}).text.split('\n')[8].split(':')[1].split('/')).split('(')[0]
    elif film_name == "二十二":
        # 编剧
        scriptwriter = '无'
        # 主演
        actor = '无'
        # 类型
        filmType = child_page.find(attrs={'id': 'info'}).text.split('\n')[2].split(':')[1].split('/')
        # 制片国家/地区
        area = child_page.find(attrs={'id': 'info'}).text.split('\n')[3].split(':')[1].split('/')
        # 语言
        language = child_page.find(attrs={'id': 'info'}).text.split('\n')[4].split(':')[1].split('/')
        # 上映日期
        initialReleaseDate = \
            min(child_page.find(attrs={'id': 'info'}).text.split('\n')[5].split(':')[1].split('/')).split('(')[0]
    else:
        # 制片国家/地区
        area = child_page.find(attrs={'id': 'info'}).text.split('\n')[5].split(':')[1].split('/')
        # 语言
        language = child_page.find(attrs={'id': 'info'}).text.split('\n')[6].split(':')[1].split('/')
        # 上映日期
        initialReleaseDate = \
            min(child_page.find(attrs={'id': 'info'}).text.split('\n')[7].split(':')[1].split('/')).split('(')[0]
    # 片长
    runtime = child_page.find(attrs={'property': 'v:runtime'}).text
    # 评分(平均分)
    rating_num = child_page.find(attrs={'property': 'v:average'}).text
    # 五星百分比
    stars5_rating_per = child_page.find(attrs={'class': 'rating_per'}).text
    # 评价人数
    rating_people = child_page.find(attrs={'property': 'v:votes'}).text
    film_info = [rank, film_name, director, scriptwriter, actor, filmType, area, language, initialReleaseDate, runtime,
                 rating_num, stars5_rating_per, rating_people]
    df = pd.DataFrame([film_info])
    # 写入文件
    df.to_csv("douban_top250.csv", mode='a', header=False, index=False)
    print(f"top{film_info[0]}爬取完成")
    resp.close()


if __name__ == '__main__':
    for i in range(10):
        one_page(f"https://movie.douban.com/top250?start={i * 25}&filter=")
        time.sleep(random.randint(1, 2))
    print("url提取完毕!")

    # for i in range(len(page_url)):
    #     childUrl = page_url[i]
    #     child(childUrl)
    
    with ThreadPoolExecutor(50) as t:
        for j in range(len(page_url)):
            childUrl = page_url[j]
            t.submit(child, childUrl)
    print("爬取完毕！")
    # 添加表头
    # 表头
    head = ['rank', 'film_name', 'director', 'scriptwriter', 'actor', 'filmType', 'area', 'language',
            'initialReleaseDate',
            'runtime', 'rating_num', 'stars5_rating_per', 'rating_people']

    # 写入数据库
    print("正在写入数据库...")
    # 建立连接
    engine = create_engine("mysql+mysqlconnector://root:root@localhost:3306/douban?charset=utf8")
    conn = engine.connect()
    # 读取csv
    df = pd.read_csv(r'douban_top250.csv', header=None, names=head)
    df.sort_values('rank', inplace=True)
    df = df.replace('无', np.NaN)
    # 写入sql 建立主键rank
    df.to_sql("douban_top250", conn, index=False, index_label='rank')
    conn.execute("""ALTER TABLE `{}` ADD PRIMARY KEY (`{}`);""".format('douban_top250', 'rank'))
    conn.close()
    print("写入数据库成功!")
