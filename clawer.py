#!/usr/bin/env python
#_*_coding:utf-8_*_

 
import requests
from lxml import etree
import json
import pymysql
from queue import Queue
import threading
import time
 
gCondition = threading.Condition()
 
HEADERS = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
    'Referer':'https://guangzhou.zbj.com/'
}
 
company_nums = 0
is_exists_company = []
 
class Producer(threading.Thread):
    def __init__(self,page_queue,company_url_queue,company_nums,is_exists_company,*args,**kwargs):
        super(Producer,self).__init__(*args,**kwargs)
        self.page_queue = page_queue
        self.company_url_queue = company_url_queue
        self.company_nums = company_nums
        self.is_exists_company = is_exists_company
 
    def run(self):
        while True:
            if self.page_queue.empty():
                break
            self.parse_url(self.page_queue.get())
 
 
    def parse_url(self,url):
        company_url_list = self.get_company_urls(url)
        for company in company_url_list:
            gCondition.acquire()
            if company in self.is_exists_company:
                gCondition.release()
                continue
            else:
                self.is_exists_company.append(company)
                self.company_nums += 1
            print('已经存入{}家公司'.format(self.company_nums))
            gCondition.release()
            text = getHTMLText(company)
            html = etree.HTML(text)
            lis = html.xpath("//ul[@class='witkeyhome-nav clearfix']//li[@class=' ']")
            if len(lis) == 0:
                self.company_url_queue.put(company)
                continue
            for li in lis:
                try:
                    if li.xpath(".//text()")[1] == '人才档案':
                        rcda_url = ('https://profile.zbj.com' + li.xpath("./a/@href")[0]).split('/salerinfo.html')[
                                       0] + '?isInOldShop=1'
                        self.company_url_queue.put(rcda_url)
                        break
                    else:continue
                except:pass  # 有一些网站的li标签是空的，因此会报错，pass掉就好
 
    def get_company_urls(self,url):
        companies_list = []
        text = getHTMLText(url)
        html = etree.HTML(text)
        h4s = html.xpath("//h4[@class='witkey-name fl text-overflow']/a/@href")
        for h4 in h4s:
            company_url = 'https:' + h4
            companies_list.append(company_url)
        return companies_list
 
 
 
 
 
class Consunmer(threading.Thread):
 
    def __init__(self,company_url_queue,page_queue,*args,**kwargs):
        super(Consunmer, self).__init__(*args,**kwargs)
        self.company_url_queue = company_url_queue
        self.page_queue = page_queue
 
    def run(self):
        while True:
            if self.company_url_queue.empty() and self.page_queue.empty():
                break
            company_url = self.company_url_queue.get()
            self.get_and_write_company_details(company_url)
            print(company_url + '写入完成')
 
    def get_and_write_company_details(self,url):
        conn = pymysql.connect(host="localhost", user="root", password="", database="zhubajie",port=3306, charset='utf8')
        cursor = conn.cursor()  # 连接数据库放在线程主函数中的，如果放在函数外面，就会导致无法连接数据库
 
        company_url = url
        text = getHTMLText(url)
        html = etree.HTML(text)
        company_name = html.xpath("//h1[@class='title']/text()")[0]
        try:
            grade = html.xpath("//div[@class='ability-tag ability-tag-3 text-tag']/text()")[0].strip()
        except:
            grade = html.xpath("//div[@class='tag-wrap tag-wrap-home']/div/text()")[0].replace('\n', '')
 
        lis = html.xpath("//ul[@class='ability-wrap clearfix']//li")
        score = float(lis[0].xpath("./div/text()")[0].strip())
        profit = float(lis[1].xpath("./div/text()")[0].strip())
        good_comment_rate = float(lis[2].xpath("./div/text()")[0].strip().split("%")[0])
        try:
            again_rate = float(lis[4].xpath("./div/text()")[0].strip().split("%")[0])
        except:
            again_rate=0.0
        try:
            finish_rate = float(lis[4].xpath("./div/text()")[0].strip().split("%")[0])
        except:
            finish_rate = 0.0
 
        company_info = html.xpath("//div[@class='conteng-box-info']//text()")[1].strip().replace("\n", '')
        skills_list = []
        divs = html.xpath("//div[@class='skill-item']//text()")
        for div in divs:
            if len(div) >= 3:
                skills_list.append(div)
        good_at_skill = json.dumps(skills_list, ensure_ascii=False)
 
        try:
            divs = html.xpath("//div[@class='our-info']//div[@class='content-item']")
            build_time = divs[1].xpath("./div/text()")[1].replace("\n", '')
            address = divs[3].xpath("./div/text()")[1].replace("\n", '')
        except:
            build_time = '暂无'
            address = '暂无'
 
        sql = """
        insert into zhubajie (id,company_name,company_url,grade,score,profit,good_comment_rate,again_rate,company_info,good_at_skill,build_time,address) values(null,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                """
 
        cursor.execute(sql, (
        company_name, company_url, grade, score, profit, good_comment_rate, again_rate, company_info, good_at_skill,
        build_time, address))
        conn.commit()
 
 
def getHTMLText(url):
    resp = requests.get(url,headers=HEADERS)
    resp.encoding='utf-8'
    return resp.text
 
def get_categories_url(url):
    details_list = []
    text = getHTMLText(url)
    html = etree.HTML(text)
    divs = html.xpath("//div[@class='channel-service-grid-inner']//div[@class='channel-service-grid-item' or @class='channel-service-grid-item second']")
    for div in divs:
        detail_url = div.xpath("./a/@href")[0]
        details_list.append(detail_url)
    return details_list
 
 
 
 
def main():
    second_page_num = {'https://guangzhou.zbj.com/wzkf/p.html':34,
                      'https://guangzhou.zbj.com/ydyykf/p.html':36,
                      'https://guangzhou.zbj.com/rjkf/p.html':37,
                      'https://guangzhou.zbj.com/uisheji/p.html':35,
                      'https://guangzhou.zbj.com/saas/p.html':38,
                      'https://guangzhou.zbj.com/itfangan/p.html':39,
                      'https://guangzhou.zbj.com/ymyfwzbj/p.html':40,
                      'https://guangzhou.zbj.com/jsfwzbj/p.html':40,
                      'https://guangzhou.zbj.com/ceshifuwu/p.html':40,
                      'https://guangzhou.zbj.com/dashujufuwu/p.html':40
                      }
    global company_nums
    company_url_queue = Queue(100000)
    page_queue = Queue(1000)
    categories_list = get_categories_url('https://guangzhou.zbj.com/it')
    for category in categories_list:
        text = getHTMLText(category)
        html = etree.HTML(text)
        pages = int(html.xpath("//p[@class='pagination-total']/text()")[0].split("共")[1].split('页')[0])
        j = second_page_num[category]
        for i in range(1,pages+1):
            if i == 1:
                page_queue.put(category)
            elif i == 2:
                page_url = category.split('.html')[0] +'k'+str(j) +'.html'
                page_queue.put(page_url)
            else:
                page_url = category.split('.html')[0] + 'k' + str(j+40*(i-2)) + '.html'
                page_queue.put(page_url)
            print('{}的第{}页已经保存到队列中'.format(category,i))
            time.sleep(1)
 
    print('url存入完成，多线程开启')
 
    for x in range(5):
        t = Producer(page_queue,company_url_queue,company_nums,is_exists_company)
        t.start()
 
    for x in range(5):
        t = Consunmer(company_url_queue,page_queue)
        t.start()
 
 
if __name__ == '__main__':
    main()