"""
重庆大学新闻网爬虫配置初始化程序。
"""

import pymysql
import datetime
import requests
from lxml import etree
from news_cqu import task_id, spider_url, conn, sleep_time, headers

# mysql 插入
# 插入spider任务表
insert_task = '''
INSERT INTO t_spider_task VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''
# 插入spider url正则表达式配置表
insert_conf = '''
INSERT INTO t_spider_conf VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s)
'''
# 插入spider xpath配置表
insert_config_xpath = '''
INSERT INTO t_spider_config_xpath VALUES (NULL, %s, %s, %s, %s)
'''


# 获取配置表的id，赋值给结果表
def get_conf_id(module_name=None):
    if module_name:
        cur.execute("SELECT id FROM t_spider_conf WHERE moduleName like %s ", '%' + module_name + '%')
    conf_id = cur.fetchone()
    conf_id = conf_id[0]

    return conf_id

# 插入config_xpath表
def insert_table(xpath, name):
    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if name[:3] == '新闻类' or name[:3] =='快讯类':
        conf_id = get_conf_id('所有栏目')
    elif name[:2] == '快讯' or name[:2] =='专题':
        conf_id = get_conf_id(name[:2])
    else:
        conf_id = get_conf_id(name[:4])

    cur.execute(insert_config_xpath, (conf_id, xpath, name, time_now))
    conn.commit()


# config_xpath表初始化，以便之后的函数读取
# 添加xpath的name命名格式为：栏目类别名+归档元数据名+xpath
# 例如，要保存新闻模块的关键词的xpath，xpath的name应该为‘新闻模块关键词xpath’
def config_xpath_initialization():
    # 插入所有栏目的xpath
    insert_table('/html/body/div[@class="row navbar"]/div/ul/li[@class="shide"]/a/@href', '所有栏目URL的xpath')

    insert_table('/html/body/div[@class="row navbar"]/div/ul/li[?]/a/text()', '新闻类栏目标题xpath')
    insert_table('/html/body/div[@class="row"]/div/div[@class="dnav"]/a[2]/text()', '快讯类栏目标题xpath')

    insert_table('//div[@class="content"]/div[@class="title"]/a/text()', '新闻模块标题xpath')
    insert_table('//div[@class="dinfob"]/div/span[2]/text()', '新闻模块发布时间xpath')
    insert_table('//*[@class="afooter"]/div[@class="tags"]/a/text()', '新闻模块关键词xpath')
    insert_table('//div[@class="dinfoa"]/p[1]/a[1]/text()', '新闻模块作者所属部门xpath')
    insert_table('//div[@class="dinfoa"]/p[1]/a[2]/text()', '新闻模块作者xpath')
    insert_table('//div[@class="abstract"]/div[@class="adetail"]/text()', '新闻模块摘要xpath')
    insert_table('//*[@class="acontent"]/p//text()', '新闻模块具体新闻内容xpath')
    insert_table('//div[@class="content"]/div[@class="title"]/a/@href', '新闻模块网址xpath')
    insert_table('//div[@class="side"]/div[@class="authora"]/div[@class="head"]'
                 '/div[@class="headinfo"]/span[@class="name"]/text()', '新闻模块责任编辑xpath')

    insert_table('//div[@class="content"]/div[@class="title"]/a/text()', '媒体重大标题xpath')
    insert_table('//div[@class="content"]/div[@class="title"]/a/@href', '媒体重大网址xpath')
    insert_table('//div[@class="rdate"]/text()', '媒体重大发布时间xpath')
    insert_table('//*[@class="afooter"]/div[@class="tags"]/a/text()', '媒体重大关键词xpath')
    insert_table('//div[@class="dinfoa"]/p[1]/span/text()', '媒体重大作者所属单位xpath')
    insert_table('//div[@class="dinfoa"]/p[1]/text()', '媒体重大作者xpath')
    insert_table('//div[@class="abstract"]/div[@class="adetail"]/text()', '媒体重大摘要xpath')
    insert_table('//*[@class="acontent"]/p//text()', '媒体重大具体新闻内容xpath')

    insert_table('//*[@class="content w100"]/div[@class="title"]/a/@href', '通知公告简报网址xpath')
    insert_table('//*[@class="content w100"]/div[@class="title"]/a/text()', '通知公告简报标题xpath')
    insert_table('/html/body/div[@class="row"]/div[@class="container detail"]/div[@class="content"]'
                 '/div[@class="acontent"]/div/strong/text()[2]', '通知公告简报发布时间xpath')
    insert_table('//*[@class="afooter"]/div[@class="tags"]/a/text()', '通知公告简报关键词xpath')
    insert_table('//*[@class="acontent"]/p//text()', '通知公告简报具体内容xpath')
    insert_table('//*[@class="dinfo"]/div[@class="dinfoa"]/p[2]/text()', '通知公告简报责任编辑xpath')
    insert_table('//*[@class="acontent"]/p[@style="line-height: 16px;"]/a/text()', '通知公告简报附件名称xpath')
    insert_table('//*[@class="acontent"]/p[@style="line-height: 16px;"]/a/@href', '通知公告简报附件地址xpath')

    insert_table('//*[@class="content w100"]/div[@class="title"]/a/@href', '学术预告网址xpath')
    insert_table('//*[@class="content w100"]/div[@class="title"]/a/text()', '学术预告标题xpath')
    insert_table('/html/body/div[@class="row"]/div[@class="container newslist"]/div[@class="container detail"]'
                 '/div[@class="content"]/div[@class="acontent"]/h3/text()', '学术预告副标题xpath')
    insert_table('/html/body/div[@class="row"]/div[@class="container newslist"]/div[@class="container detail"]'
                 '/div[@class="content"]/div[@class="dinfo"]/p[1]/text()', '学术预告发生时间（讲座时间）xpath')
    insert_table('/html/body/div[@class="row"]/div[@class="container newslist"]/div[@class="container detail"]'
                 '/div[@class="content"]/div[@class="dinfo"]/p[2]/text()', '学术预告地点xpath')
    insert_table('/html/body/div[@class="row"]/div[@class="container newslist"]/div[@class="container detail"]'
                 '/div[@class="content"]/div[@class="acontent"]/p[1]/text()', '学术预告主讲人xpath')
    insert_table('//*[@class="afooter"]/div[@class="tags"]/a/text()', '学术预告关键词xpath')
    insert_table('//div[@class="dinfoa"]/p[1]/a[1]/text()', '学术预告作者所属部门xpath')
    insert_table('//div[@class="dinfoa"]/p[1]/a[2]/text()', '学术预告作者xpath')
    insert_table('//*[@class="acontent"]/p//text()', '学术预告具体内容xpath')
    insert_table('//*[@class="dinfo"]/div[@class="dinfoa"]/p[2]/text()', '学术预告责任编辑xpath')

    insert_table('//*[@class="content w100"]/div[@class="title"]/a/text()', '快讯标题xpath')
    insert_table('//div[@class="rdate"]/text()', '快讯发布时间xpath')
    insert_table('//*[@class="content w100"]/div[@class="abstract1"]/text()', '快讯具体内容xpath')

    insert_table('//*[@class="col-lg-4"]/a/@href', '专题网址xpath')
    insert_table('//*[@class="col-lg-4"]/a/strong/text()', '专题标题xpath')


def config_initialization():
    r = requests.get(spider_url, headers=headers)
    r.encoding = 'UTF-8'
    html = etree.HTML(r.text)

    news_heading_list = []

    try:
        news_heading_list = html.xpath('//ul[@class="nav"]/li[@class="shide"]/a/text()')
        # 将主页的url去掉
        news_heading_list.remove(news_heading_list[0])
        # 增加快讯，专题，所有栏目 三个板块
        news_heading_list.append('快讯')
        news_heading_list.append('专题')
        news_heading_list.append('所有栏目')
    except IndexError:
        print("xpath配置错误！")

    module_name = '新闻模块（包括综合新闻、教学科研、招生就业、交流合作、校园生活栏目）'
    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur.execute(insert_conf, (task_id, spider_url, sleep_time, r'http://\S*/\w*-\d+.html',
                              r'http://\S*/show-\d*-\d*-\d*.html', module_name, time_now, time_now))

    for each_module in news_heading_list:
        if each_module == '媒体重大' or each_module =='通知公告简报' \
                or each_module =='学术预告' or each_module =='快讯' or each_module =='专题' or each_module =='所有栏目':
            time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cur.execute(insert_conf, (task_id, spider_url, sleep_time, r'http://\S*/\w*-\d+.html',
                                      r'http://\S*/show-\d*-\d*-\d*.html', each_module, time_now, time_now))

if __name__ == '__main__':
    cur = conn.cursor()
    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # cur.execute(insert_task, (task_id, '重大新闻网新闻爬取', '0 30 2 * * ?', '', 0, 0, None, time_now))
    # conn.commit()
    # print('task表初始化成功！')
    
    config_initialization()
    conn.commit()
    print('config表初始化成功！')

    config_xpath_initialization()
    conn.commit()
    print('config_xpath表初始化成功！')

    conn.close()
