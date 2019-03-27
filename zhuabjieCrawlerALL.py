# coding=utf-8
# install BeautifulSoup: sudo easy_install beautifulsoup4
# python2.7.6

from urllib import request, parse
from bs4 import BeautifulSoup
import csv
import logging
import glob
import json
import codecs

class ZhubajieCrawler(object):
    def __init__(self):
        # 保存到成员属性中，这样在其他的方法中就可以直接使用
        # self.jl = jl
        # self.kw = kw
        # self.start_page = start_page
        # self.end_page = end_page

        self.items = []

    def handle_request(self, page):
        url = 'https://sou.zhaopin.com/jobs/searchresult.ashx?'
        data = {
            'jl': self.jl,
            'kw': self.kw,
            'p': page,
        }
        query_string = request.parse.urlencode()
        # 拼接url
        url += query_string
        # print(url)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
        }
        return request.Request(url=url, headers=headers)
    def parse_content(self, content):
        try:
            soup = BeautifulSoup(content, features="html.parser")
            # 首先找到所有的需求
            demandList = soup.find_all("div", class_="demand")[1:]
            # 遍历当面页面所有的项目
            for demand in demandList:
                pro_id = demand.a['data-taskid']
                name = demand.find('span', class_='xq-title').text.strip()
                href = 'https:' + demand.a['href']
                state = demand.find('span', class_='mode-icons').text.strip()
                tags = demand.find('span', class_='d-tags').text.strip().split('\n')
                price = demand.find('b', class_='d-base-price').text.strip()
                des = demand.find('p', class_='d-des').text.strip().replace('\n','')

                item = {
                    # pro_id, name, href, state, tags, price, des
                    'pro_id': pro_id,
                    'name': name,
                    'href': href,
                    'state': state,
                    'tags': tags,
                    'price': price,
                    'des': des
                }
                print(item)
                self.items.append(item)
        except (IOError ,ZeroDivisionError) as e:
            print(e.message)


    def run(self, headers,fileName):
        # 循环
        for num in range(1, 3):  # 按编号抓取所有任务。
            url = 'https://task.zbj.com/page' + str(num) + '.html'
            print('正在爬取第%s页......' % num)
            req = request.Request(url, headers=headers)  # 在HTTP Request请求中添加headers以模拟登录。
            try:  # 忽略异常。
                page = request.urlopen(req, timeout=10).read()  # 发送请求读取页面，设置等待10秒超时。
                self.parse_content(page)
                self.wirteFile(fileName)

            except:
                pass


    def wirteFile(self,fileName):
        # 写入文件
        print('开始写入文件')
        # with open(fileName, 'a') as file:
        #     json.dump((self.items), file)
        #     file.close
        #     print('写入文件成功')
        with open(fileName, 'a') as csvfile:
            spanwriter = csv.writer(csvfile, dialect='excel')
            items = self.items
            for item in items:
                spanwriter.writerow([item['pro_id'],
                                     item['name'],
                                     item['href'],
                                     item['state'],
                                     item['tags'],
                                     item['price'],
                                     item['des']])
        csvfile.close
        print('写入文件成功')
        self.items = []
def main():
    fileName = 'task.csv'  # 根据范围设定新csv文件名。
    fileJson = 'task.json' # 根据范围设定新csv文件名。
    with open(fileName, 'a') as csvfile:  # 创建文件，添加第一行任务属性
        spanwriter = csv.writer(csvfile, dialect='excel')
        spanwriter.writerow(
            ['编号', '项目名称', '项目链接', '项目状态', '项目标签', '项目价格', '项目描述'])
    csvfile.close  # 写入列头后关闭文件。

    cookie = 'PHPSESSID=45br8ifsdokgiaacjpeejq4sd4; _analysis=7140gWp99UJei9%2FPhANHC8B6ZPTC2kBz2UOIlSC8%2FdeJiadFz3JtFavtFdgaqamPT2o%2FCE6IhZXLiSqt3j2s0H5PK21p; fvtime=3102qeLXAUOcgD3AK0G29td7%2FezaZWb%2FxZIsVVLrPp9q7hNho6Kx; uniqid=15f9d1a5e43e933281047dafa6e380c1; _uq=a888ccfed3bc0467956a341e89cc235e; zbj_advert_zh=1; _uv=1; userkey=9t30H1%2FhPxH21cs750dsUjQ%2BZUeSeELw0xa4N7ROHg0P%2BfbEY5H5IDDBxlMUaJvBPiTT4de%2FDdSrKozZ6VkW%2FwyM1vA%2B4h1C3l3plXhSzTr4%2FZl4ZpyYwX7dkk%2BWsP5hFYKpGohuGszdhtCzp86Gq8RF3fnjxveHU4TlELHvDEWW60MDtdoINwIXtU9r8qSpgRn9oFIQlcOGjn9Zl3Xu%2FodE46DNogcYKu4GFHIeNH%2FowA5fN3sqJu0eIZB%2BaREIhu6B; userid=12596972; nickname=today545; brandname=today545; Hm_lvt_20e969969d6befc1cc3a1bf81f6786ef=1430901078; Hm_lpvt_20e969969d6befc1cc3a1bf81f6786ef=1430901108'  # 有效登录cookie。

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36',
        'cookie': cookie
    }  # 用chrome浏览器登录后,利用开发者工具查看NetWork主页面分析获取的headers。
    zbjCrawler = ZhubajieCrawler()
    zbjCrawler.run(headers,fileName)
    # zbjCrawler.run(headers,fileJson)



if __name__ == '__main__':
    main()