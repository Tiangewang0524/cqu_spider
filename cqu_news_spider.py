"""
爬取原网页的html，过滤新闻内容并重新拼接，保留原网页样式。
"""

import pymysql
import datetime
import requests
from lxml import etree
import pdfkit
import os
import time
import json
import re
# 敏感词过滤类，AC自动机
import Ac_auto

# 任务id
task_id = 2
# 爬取的地址和名称
spider_url = 'https://news.cqu.edu.cn/newsv2/'
spider_name = '重大新闻网'
# 每页最大爬取新闻数
# i_news = 1
# 睡眠时间
sleep_time = 0.1
# mysql登录信息
conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    passwd='123456',
    db='spider_test',
    use_unicode=True,
    charset="utf8mb4"
)

# mysql 插入
# 插入spider结果表
insert_result = '''
INSERT INTO t_spider_result VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
'''

# 全局字典变量，以键值对（键：对应URL，值：标题）形式存储爬取的数据记录。
dict_data = dict()

# pdfkit配置
confg = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')


# 伪装http请求头部
headers = {
    'User-Agent':
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;'
}


# 从数据库配置表里获取xpath, 并解析获取内容
def get_xpath_content(html, xpath_name):
    # 去掉空格字符串函数
    def not_empty(s):
        return s and s.strip()

    cur.execute("SELECT xpath FROM t_spider_config_xpath WHERE name = %s", xpath_name)
    xpath = cur.fetchone()
    xpath = xpath[0]

    content = html.xpath(xpath)

    # 对content做处理, 元素小于2的处理成字符串, 并去掉首尾多余的空格, 大于2的去掉空格字符串
    if len(content) < 2:
        content = ''.join(content)
        content = content.strip()
    else:
        content = list(filter(not_empty, content))

    return content


# 获取配置表的id，赋值给结果表
def get_conf_id(module_name=None):
    if module_name:
        cur.execute("SELECT id FROM t_spider_conf WHERE moduleName like %s ", '%' + module_name + '%')
    conf_id = cur.fetchone()
    conf_id = conf_id[0]

    return conf_id


# 查找所有栏目的url（板块url），并保存
def all_urls_list(f_data):
    global dict_data
    # 读取字典中的数据
    f_data.seek(0, 0)
    content = f_data.read()
    if content:
        dict_data = json.loads(content)

    heading = '重大新闻网'

    # 存储index的记录，放进字典和数据库，如果已经存在，则不存储
    judge = spider_url in dict_data.keys()
    if not judge:
        dict_data[spider_url] = heading

        # 创建文件夹
        # 先判断文件夹是否存在，不存在则创建文件夹
        now_dir = os.getcwd()
        new_dir = now_dir + '\\' + heading + '首页'
        dir_judge = os.path.exists(new_dir)
        if not dir_judge:
            os.mkdir(new_dir)

        res = requests.get(spider_url, headers=headers)
        res.encoding = 'UTF-8'
        raw_html = res.text

        html_filter = sensitive_word_filter(raw_html)
        timestamp = round(time.time())
        html_file = new_dir + '\\' + str(timestamp) + '.html'
        pdf_file = new_dir + '\\' + str(timestamp) + '.pdf'

        # 获取配置表的id，赋值给结果表
        conf_id = get_conf_id('所有栏目')

        time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cur.execute(insert_result, (conf_id, 'index', spider_url, html_filter, html_file, pdf_file, time_now, heading, None, ''))
        conn.commit()
        json_data = json.dumps(dict_data)
        f_data.seek(0, 0)
        f_data.write(json_data)

        try:
            with open(html_file, 'w+', encoding='UTF-8') as f1:
                f1.write(html_filter)
            # html转pdf
            pdfkit.from_url(spider_url, pdf_file, configuration=confg)
            print('《{}》 的首页已储存，转换pdf格式已成功。'.format(heading))
            time.sleep(sleep_time)
        except IOError:
            print("Warning: wkhtmltopdf读取文件失败, 可能是网页无法打开或者图片/css样式丢失。")
    else:
        print('{} 首页记录已爬取过且保存在数据库中！'.format(heading))

    r = requests.get(spider_url, headers=headers)
    r.encoding = 'UTF-8'
    html = etree.HTML(r.text)

    news_heading_url_list = []

    try:
        news_heading_url_list = get_xpath_content(html, '所有栏目URL的xpath')
        # 将主页的url去掉
        news_heading_url_list.remove(news_heading_url_list[0])
        # 增加快讯，专题两个板块
        news_heading_url_list.append('https://news.cqu.edu.cn/newsv2/list-15.html')
        news_heading_url_list.append('http://news.cqu.edu.cn/kjcd/')
    except IndexError:
        print("xpath配置错误！")
    except etree.XPathEvalError:
        print("数据库里未找到记录！")

    # print(news_heading_url_list)

    return news_heading_url_list


# 查找每个栏目/板块下的每一页的url（列表url），并保存
# 适用于第一大类：新闻模块，第二大类：媒体重大，第三大类：通知公告简报，第四大类：学术预告, 第五大类：快讯
def get_url_list(url, all_urls, f_data):
    global dict_data
    # 读取字典中的数据
    f_data.seek(0, 0)
    content = f_data.read()
    if content:
        dict_data = json.loads(content)

    url_list = []
    r = requests.get(url, headers=headers)
    r.encoding = 'UTF-8'
    html = etree.HTML(r.text)

    news_heading = ''
    # 获取板块在news_heading_url_list的序号，并获取板块名称以及板块下总的新闻数目
    # 对 快讯板块做处理：
    if url == 'https://news.cqu.edu.cn/newsv2/list-15.html':
        try:
            news_heading = get_xpath_content(html, '快讯类栏目标题xpath')
            news_heading = ''.join(news_heading)
            # print(news_heading)
        except IndexError:
            print("xpath配置错误！")
        except etree.XPathEvalError:
            print("数据库里未找到记录！")

        temp_url = url
    else:
        cur.execute("SELECT xpath from t_spider_config_xpath where name = %s", '新闻类栏目标题xpath')
        xpath = cur.fetchone()
        xpath = xpath[0]

        # 根据不同的栏目指定不同的xpath
        index = all_urls.index(url)
        xpath = xpath.replace('?', str(index + 2))

        try:
            news_heading = html.xpath(xpath)
            news_heading = ''.join(news_heading)
            # print(news_heading)
        except IndexError:
            print("xpath配置错误！")
        except etree.XPathEvalError:
            print("数据库里未找到记录！")
        temp_url = url + '?page=1'

    # 查找最大页数
    page = html.xpath('/html/body/div[@class="row"]/div/div[@class="lists"]/div[@class="page"]/a[12]/text()')
    page = ''.join(page)
    # print(page)

    max_page = int(page)

    # 存储list第一页的记录，放进字典和数据库，如果已经存在，则不存储
    judge = temp_url in dict_data.keys()
    if not judge:
        dict_data[temp_url] = news_heading

        # 创建文件夹
        # 先判断文件夹是否存在，不存在则创建文件夹
        now_dir = os.getcwd()
        new_dir = now_dir + '\\' + news_heading
        dir_judge = os.path.exists(new_dir)
        if not dir_judge:
            os.mkdir(new_dir)

        res = requests.get(temp_url, headers=headers)
        res.encoding = 'UTF-8'
        raw_html = res.text

        html_filter = sensitive_word_filter(raw_html)
        timestamp = round(time.time())
        html_file = new_dir + '\\' + str(timestamp) + '.html'
        pdf_file = new_dir + '\\' + str(timestamp) + '.pdf'

        # 获取配置表的id，赋值给结果表
        conf_id = get_conf_id(news_heading)

        time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cur.execute(insert_result, (conf_id, 'list', temp_url, html_filter, html_file, pdf_file, time_now, news_heading, None, ''))
        conn.commit()
        json_data = json.dumps(dict_data)
        f_data.seek(0, 0)
        f_data.write(json_data)

        try:
            with open(html_file, 'w+', encoding='UTF-8') as f1:
                f1.write(html_filter)
            # html转pdf
            pdfkit.from_url(temp_url, pdf_file, configuration=confg)
            print('栏目 《{}》 的首页已储存，转换pdf格式已成功。'.format(news_heading))
            time.sleep(sleep_time)
        except IOError:
            print("Warning: wkhtmltopdf读取文件失败, 可能是网页无法打开或者图片/css样式丢失。")
    else:
        print('{} 栏目 首页记录已爬取过且保存在数据库中！'.format(news_heading))

    # 对 快讯板块做处理：
    if url == 'https://news.cqu.edu.cn/newsv2/list-15.html':
        for i in range(1, max_page + 1):
            temp_url = url[:-5] + '-' + str(i) + '.html'
            url_list.append(temp_url)
    else:
        for i in range(1, max_page + 1):
            # print('爬取网上新闻的第{}页......'.format(i))
            temp_url = url + '?page=' + str(i)
            url_list.append(temp_url)
    # print(url_list)
    return url_list


# 查找专题 栏目下的每一页的url（列表url），并保存, 返回一个字典文件。
def get_topic_url_list(url, f_data):
    global dict_data
    # 读取字典中的数据
    f_data.seek(0, 0)
    content = f_data.read()
    if content:
        dict_data = json.loads(content)

    url_dict = dict()
    r = requests.get(url, headers=headers)
    r.encoding = 'UTF-8'
    html = etree.HTML(r.text)

    news_heading = '专题'

    # 存储专题list的记录，放进字典和数据库，如果已经存在，则不存储
    judge = url in dict_data.keys()
    if not judge:
        dict_data[url] = news_heading

        # 创建文件夹
        # 先判断文件夹是否存在，不存在则创建文件夹
        now_dir = os.getcwd()
        new_dir = now_dir + '\\' + news_heading
        dir_judge = os.path.exists(new_dir)
        if not dir_judge:
            os.mkdir(new_dir)

        res = requests.get(url, headers=headers)
        res.encoding = 'UTF-8'
        raw_html = res.text

        html_filter = sensitive_word_filter(raw_html)
        timestamp = round(time.time())
        html_file = new_dir + '\\' + str(timestamp) + '.html'
        pdf_file = new_dir + '\\' + str(timestamp) + '.pdf'

        # 获取配置表的id，赋值给结果表
        conf_id = get_conf_id(news_heading)

        time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cur.execute(insert_result, (conf_id, 'list', url, html_filter, html_file, pdf_file, time_now, news_heading, None, ''))
        conn.commit()
        json_data = json.dumps(dict_data)
        f_data.seek(0, 0)
        f_data.write(json_data)

        try:
            with open(html_file, 'w+', encoding='UTF-8') as f1:
                f1.write(html_filter)
            # html转pdf
            pdfkit.from_url(url, pdf_file, configuration=confg)
            print('栏目 《{}》 的主页已储存，转换pdf格式已成功。'.format(news_heading))
            time.sleep(sleep_time)
        except IOError:
            print("Warning: wkhtmltopdf读取文件失败, 可能是网页无法打开或者图片/css样式丢失。")
    else:
        print('{} 栏目 主页记录已爬取过且保存在数据库中！'.format(news_heading))

    try:
        topic_urls_list = get_xpath_content(html, '专题网址xpath')
        topic_names_list = get_xpath_content(html, '专题标题xpath')
        # print(topic_urls_list)
        # print(topic_names_list)

        # 首页4个专题的URL添加进topic_urls_list, 将四个专题标题名添加进topic_names_list
        topic_name = ['毕业季|青春不落幕 友谊不散场', '辉煌70年•追梦重大人', '不忘初心 牢记使命', '一带一路年会']
        for i in range(4, 8):
            topic_urls_list.append('http://news.cqu.edu.cn/newsv2/index.php?m=special&c=index&specialid=8' + str(i))
            topic_names_list.append(topic_name[(i - 4)])

        # 给每个专题标题名添加’专题_‘进行区分
        temp_list = []
        for each in topic_names_list:
            temp_list.append('专题_' + each)

        topic_names_list = temp_list
        url_dict = dict(zip(topic_names_list, topic_urls_list))

        # 字典key:专题标题，value:专题链接
        # print(url_dict)

    except IndexError:
        print("xpath配置错误！")
    except etree.XPathEvalError:
        print("数据库里未找到记录！")

    return url_dict


# 读取新闻模块每个页面的url，获取新闻模块的每条新闻的归档元数据，并将页面转成pdf格式保存
def get_news_info(url_list, module_url, all_urls, f_data):
    global dict_data
    # 读取字典中的数据
    f_data.seek(0, 0)
    content = f_data.read()
    if content:
        dict_data = json.loads(content)

    # 获取配置表的id，赋值给结果表
    conf_id = get_conf_id('新闻模块')

    # 新闻模块新闻数累加器
    sum_i = 0

    # 新闻模块页数计数器
    page = 1

    # 获取栏目名称
    news_heading = ''

    dict_news = dict()
    dict_news = {'网站名称': spider_name, '网站域名': spider_url}


    r = requests.get(module_url, headers=headers)
    r.encoding = 'UTF-8'
    html = etree.HTML(r.text)

    cur.execute("SELECT xpath from t_spider_config_xpath where name = %s", '新闻类栏目标题xpath')
    xpath = cur.fetchone()
    xpath = xpath[0]

    # 根据不同的栏目指定不同的xpath
    index = all_urls.index(module_url)
    xpath = xpath.replace('?', str(index + 2))

    try:
        news_heading = html.xpath(xpath)
        news_heading = ''.join(news_heading)
        # print(news_heading)
    except IndexError:
        print("xpath配置错误！")
    except etree.XPathEvalError:
        print("数据库里未找到记录！")


    # 创建文件夹
    # 先判断文件夹是否存在，不存在则创建文件夹
    now_dir = os.getcwd()
    new_dir = now_dir + '\\' + news_heading
    dir_judge = os.path.exists(new_dir)
    if not dir_judge:
        os.mkdir(new_dir)

    # 每一页的url
    for url in url_list:

        r = requests.get(url, headers=headers)
        r.encoding = 'UTF-8'
        raw_html = r.text
        html = etree.HTML(raw_html)

        links_list = get_xpath_content(html, '新闻模块网址xpath')
        title_list = get_xpath_content(html, '新闻模块标题xpath')


        # 每一条新闻的url + 每一个标题
        for each_url, title in zip(links_list, title_list):
            print('正在爬取 {} 栏目下，第 {} 页 总第 {} 条新闻。'.format(news_heading, page, sum_i + 1))
            # 存储每一个新闻模块链接URL的记录，放进字典和数据库，如果已经存在，则不存储
            judge = each_url in dict_data.keys()
            try:
                if not judge:
                    dict_data[each_url] = title

                    r = requests.get(each_url, headers=headers)
                    r.encoding = 'UTF-8'
                    raw_html = r.text
                    html = etree.HTML(raw_html)

                    html_filter = sensitive_word_filter(raw_html)
                    html_filter = path_rewrite(html_filter)
                    timestamp = round(time.time())
                    html_file = new_dir + '\\' + str(timestamp) + '.html'
                    pdf_file = new_dir + '\\' + str(timestamp) + '.pdf'

                    dict_news['所属栏目'] = news_heading

                    try:
                        cur.execute("SELECT name from t_spider_config_xpath where name like %s", '新闻模块' + '%')
                        xpath_name = cur.fetchall()
                        for each in xpath_name:
                            dict_news[each[0][4:-5]] = get_xpath_content(html, each[0])
                    except IndexError:
                        print("xpath配置错误！")
                    except etree.XPathEvalError:
                        print("数据库里未找到记录！")

                    dict_news['标题'] = title
                    dict_news['网址'] = each_url
                    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    dict_news['采集时间'] = time_now
                    dict_news['采集人'] = '档案馆'

                    if dict_news['发布时间']:
                        release_time = dict_news['发布时间']
                    else:
                        release_time = None

                    json_dict = json.dumps(dict_news, ensure_ascii=False, indent=4)
                    print(json_dict)

                    judge_identifier = not_found_judge(raw_html, r)
                    # 判断网页是不是404 not found
                    if judge_identifier:
                        cur.execute(insert_result, (conf_id, 'detail', each_url, html_filter, html_file, pdf_file,
                                                    time_now, news_heading, release_time, json_dict))
                        conn.commit()
                        json_data = json.dumps(dict_data)
                        f_data.seek(0, 0)
                        f_data.write(json_data)
                        sum_i += 1

                        with open(html_file, 'w+', encoding='UTF-8') as f1:
                            f1.write(html_filter)
                        # html转pdf
                        pdfkit.from_url(each_url, pdf_file, configuration=confg)
                        print('该新闻《{}》pdf格式已转换成功。'.format(title))
                        time.sleep(sleep_time)

                    else:
                        # 将404 not found 记录进数据库
                        html_filter = '404 not found'
                        cur.execute(insert_result, (conf_id, 'detail', each_url, html_filter, '', '',
                                                    time_now, news_heading, None, json_dict))
                        conn.commit()
                        json_data = json.dumps(dict_data)
                        f_data.seek(0, 0)
                        f_data.write(json_data)
                        print('该新闻《{}》网页不存在， 以‘404 not found’为网页内容存入数据库。'.format(title))
                        sum_i += 1
                else:
                    sum_i += 1
                    print('{} 栏目 的 第 {} 条新闻 已爬取过且保存在数据库中！'.format(news_heading, sum_i))
            except IOError:
                print("Warning: wkhtmltopdf读取文件失败, 可能是网页无法打开或者图片/css样式丢失。")
            except IndexError:
                print("该栏目《{}》下的新闻已全部爬取完！".format(news_heading))
                break

        print('第{}页已经爬取完'.format(page))
        page += 1

    print('{} 栏目下 共有{}页 {}条新闻'.format(news_heading, page - 1, sum_i))


# 读取媒体重大每个页面的url，获取媒体重大的每条新闻的归档元数据，并将页面转成pdf格式保存
def get_media_info(url_list, f_data):
    global dict_data
    # 读取字典中的数据
    f_data.seek(0, 0)
    content = f_data.read()
    if content:
        dict_data = json.loads(content)

    # 媒体重大新闻数累加器
    sum_i = 0

    # 媒体重大页数计数器
    page = 1

    # 媒体重大发布时间处理计数器
    i = 0

    news_heading = '媒体重大'

    # 获取配置表的id，赋值给结果表
    conf_id = get_conf_id(news_heading)

    dict_media = dict()
    dict_media = {'网站名称': spider_name, '网站域名': spider_url}


    # 创建文件夹
    # 先判断文件夹是否存在，不存在则创建文件夹
    now_dir = os.getcwd()
    new_dir = now_dir + '\\' + news_heading
    dir_judge = os.path.exists(new_dir)
    if not dir_judge:
        os.mkdir(new_dir)

    # 每一页的url
    for url in url_list:

        r = requests.get(url, headers=headers)
        r.encoding = 'UTF-8'
        raw_html = r.text
        html = etree.HTML(raw_html)

        links_list = get_xpath_content(html, '媒体重大网址xpath')
        title_list = get_xpath_content(html, '媒体重大标题xpath')

        release_time_list = get_xpath_content(html, '媒体重大发布时间xpath')

        # 格式化发布时间
        temp_list = []
        for each in release_time_list:
            each = each.strip()
            # print(each)
            temp_list.append(each)
        release_time_list = []
        while i < len(temp_list) - 1:
            release_time = temp_list[i] + '月' + temp_list[i + 1] + '日'
            release_time_list.append(release_time)
            i += 2
        # 将计数器清零
        i = 0

        # 每一条新闻的url + 每一条发布时间 + 每一个标题
        for each_url, release_time, title in zip(links_list, release_time_list, title_list):
            print('正在爬取 {} 栏目下，第 {} 页 总第 {} 条新闻。'.format(news_heading, page, sum_i + 1))
            # 存储每一个媒体重大链接URL的记录，放进字典和数据库，如果已经存在，则不存储
            judge = each_url in dict_data.keys()
            try:
                if not judge:
                    dict_data[each_url] = title
                    r = requests.get(each_url, headers=headers)
                    r.encoding = 'UTF-8'
                    raw_html = r.text
                    html = etree.HTML(raw_html)

                    html_filter = sensitive_word_filter(raw_html)
                    html_filter = path_rewrite(html_filter)
                    timestamp = round(time.time())
                    html_file = new_dir + '\\' + str(timestamp) + '.html'
                    pdf_file = new_dir + '\\' + str(timestamp) + '.pdf'

                    resource = ''
                    dict_media['所属栏目'] = news_heading

                    # 从数据库获取xpath, 并根据xpath获取内容
                    try:
                        cur.execute("SELECT name from t_spider_config_xpath where name like %s", news_heading + '%')
                        xpath_name = cur.fetchall()
                        for each in xpath_name:
                            # [4:-5]表示去掉开头‘媒体重大’四个字符和结尾‘xpath’五个字符
                            if each[0][4:-5] == '具体新闻内容':
                                resource = get_xpath_content(html, each[0])[-1]
                            if each[0][4:-5] == '作者所属单位':
                                department = get_xpath_content(html, each[0])[:-4]
                                dict_media[each[0][4:-5]] = department
                            else:
                                dict_media[each[0][4:-5]] = get_xpath_content(html, each[0])
                    except IndexError:
                        print("xpath配置错误！")
                    except etree.XPathEvalError:
                        print("数据库里未找到记录！")

                    dict_media['标题'] = title
                    dict_media['发布时间'] = release_time
                    dict_media['来源（转载来源）'] = resource
                    dict_media['网址'] = each_url

                    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    dict_media['采集时间'] = time_now
                    dict_media['采集人'] = '档案馆'
                    json_dict = json.dumps(dict_media, ensure_ascii=False, indent=4)
                    print(json_dict)

                    judge_identifier = not_found_judge(raw_html, r)
                    # 判断网页是不是404 not found
                    if judge_identifier:
                        cur.execute(insert_result, (conf_id, 'detail', each_url, html_filter, html_file, pdf_file,
                                                    time_now, news_heading, None, json_dict))
                        conn.commit()
                        json_data = json.dumps(dict_data)
                        f_data.seek(0, 0)
                        f_data.write(json_data)
                        sum_i += 1

                        with open(html_file, 'w+', encoding='UTF-8') as f1:
                            f1.write(html_filter)
                        # html转pdf
                        pdfkit.from_url(each_url, pdf_file, configuration=confg)
                        print('该新闻《{}》pdf格式已转换成功。'.format(title))
                        time.sleep(sleep_time)

                    else:
                        # 将404 not found 记录进数据库
                        html_filter = '404 not found'
                        cur.execute(insert_result, (conf_id, 'detail', each_url, html_filter, '', '',
                                                    time_now, news_heading, None, json_dict))
                        conn.commit()
                        json_data = json.dumps(dict_data)
                        f_data.seek(0, 0)
                        f_data.write(json_data)
                        print('该新闻《{}》网页不存在， 以‘404 not found’为网页内容存入数据库。'.format(title))
                        sum_i += 1
                else:
                    sum_i += 1
                    print('{} 栏目 的 第 {} 条新闻 已爬取过且保存在数据库中！'.format(news_heading, sum_i))
            except IOError:
                print("Warning: wkhtmltopdf读取文件失败, 可能是网页无法打开或者图片/css样式丢失。")
            except IndexError:
                print("该栏目《{}》下的媒体新闻已全部爬取完！".format(news_heading))
                break

        print('第{}页已经爬取完'.format(page))
        page += 1

    print('{} 栏目下 共有{}页 {}条媒体新闻'.format(news_heading, page - 1, sum_i))


# 读取通知公告简报每个页面的url，获取通知公告简报的每条新闻的归档元数据，并将页面转成pdf格式保存
def get_notice_info(url_list, f_data):
    global dict_data
    # 读取字典中的数据
    f_data.seek(0, 0)
    content = f_data.read()
    if content:
        dict_data = json.loads(content)

    # 通知公告数累加器
    sum_i = 0

    # 通知公告简报页数计数器
    page = 1

    news_heading = '通知公告简报'

    # 获取配置表的id，赋值给结果表
    conf_id = get_conf_id(news_heading)

    # 通知公告简报
    dict_notice = dict()
    dict_notice = {'网站名称': spider_name, '网站域名': spider_url}

    # 创建文件夹
    # 先判断文件夹是否存在，不存在则创建文件夹
    now_dir = os.getcwd()
    new_dir = now_dir + '\\' + news_heading
    dir_judge = os.path.exists(new_dir)
    if not dir_judge:
        os.mkdir(new_dir)

    # 每一页的url
    for url in url_list:

        r = requests.get(url, headers=headers)
        r.encoding = 'UTF-8'
        raw_html = r.text
        html = etree.HTML(raw_html)

        links_list = get_xpath_content(html, '通知公告简报网址xpath')
        title_list = get_xpath_content(html, '通知公告简报标题xpath')

        # 每一条通知的url + 每一个标题
        for each_url, title in zip(links_list, title_list):
            print('正在爬取 {} 栏目下，第 {} 页 总第 {} 条通知公告。'.format(news_heading, page, sum_i + 1))
            # 存储每一个学术预告链接URL的记录，放进字典和数据库，如果已经存在，则不存储
            judge = each_url in dict_data.keys()
            try:
                if not judge:
                    dict_data[each_url] = title
                    r = requests.get(each_url, headers=headers)
                    r.encoding = 'UTF-8'
                    raw_html = r.text
                    html = etree.HTML(raw_html)

                    html_filter = sensitive_word_filter(raw_html)
                    html_filter = path_rewrite(html_filter)
                    timestamp = round(time.time())
                    html_file = new_dir + '\\' + str(timestamp) + '.html'
                    pdf_file = new_dir + '\\' + str(timestamp) + '.pdf'

                    # 对跳转微信公众号文章的链接做处理
                    if 'weixin' in each_url:
                        title = html.xpath('//h2[@class="rich_media_title"]/text()')
                        title = ''.join(title)
                        title = title.strip()

                    dict_notice['所属栏目'] = news_heading

                    # 从数据库获取xpath, 并根据xpath获取内容
                    try:
                        cur.execute("SELECT name from t_spider_config_xpath where name like %s",
                                    news_heading + '%')
                        xpath_name = cur.fetchall()
                        for each in xpath_name:
                            # [6:-5]表示去掉开头‘通知公告简报’四个字符和结尾‘xpath’五个字符
                            dict_notice[each[0][6:-5]] = get_xpath_content(html, each[0])
                    except IndexError:
                        print("xpath配置错误！")
                    except etree.XPathEvalError:
                        print("数据库里未找到记录！")

                    dict_notice['标题'] = title
                    dict_notice['网址'] = each_url

                    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    dict_notice['采集时间'] = time_now
                    dict_notice['采集人'] = '档案馆'

                    if dict_notice['发布时间']:
                        release_time = dict_notice['发布时间']
                    else:
                        release_time = None

                    json_dict = json.dumps(dict_notice, ensure_ascii=False, indent=4)
                    print(json_dict)

                    judge_identifier = not_found_judge(raw_html, r)
                    # 判断网页是不是404 not found
                    if judge_identifier:
                        cur.execute(insert_result, (conf_id, 'detail', each_url, html_filter, html_file, pdf_file,
                                                    time_now, news_heading, release_time, json_dict))
                        conn.commit()
                        json_data = json.dumps(dict_data)
                        f_data.seek(0, 0)
                        f_data.write(json_data)
                        sum_i += 1

                        with open(html_file, 'w+', encoding='UTF-8') as f1:
                            f1.write(html_filter)
                        # html转pdf
                        pdfkit.from_url(each_url, pdf_file, configuration=confg)
                        print('该通知《{}》pdf格式已转换成功。'.format(title))
                        time.sleep(sleep_time)

                    else:
                        # 将404 not found 记录进数据库
                        html_filter = '404 not found'
                        cur.execute(insert_result, (conf_id, 'detail', each_url, html_filter, '', '',
                                                    time_now, news_heading, None, json_dict))
                        conn.commit()
                        json_data = json.dumps(dict_data)
                        f_data.seek(0, 0)
                        f_data.write(json_data)
                        print('该通知《{}》网页不存在， 以‘404 not found’为网页内容存入数据库。'.format(title))
                        sum_i += 1
                else:
                    sum_i += 1
                    print('{} 栏目 的 第 {} 条通知 已爬取过且保存在数据库中！'.format(news_heading, sum_i))
            except IOError:
                print("Warning: wkhtmltopdf读取文件失败, 可能是网页无法打开或者图片/css样式丢失。")
            except IndexError:
                print("该栏目《{}》下的通知公告简报已全部爬取完！".format(news_heading))
                break

        print('第{}页已经爬取完'.format(page))
        page += 1

    print('{} 栏目下 共有{}页 {}条通知公告简报'.format(news_heading, page - 1, sum_i))


# 读取学术预告每个页面的url，获取学术预告的每条新闻的归档元数据，并将页面转成pdf格式保存
def get_academic_info(url_list, f_data):
    global dict_data
    # 读取字典中的数据
    f_data.seek(0, 0)
    content = f_data.read()
    if content:
        dict_data = json.loads(content)

    # 讲座数累加器
    sum_i = 0

    # 学术预告页数计数器
    page = 1

    news_heading = '学术预告'

    # 获取配置表的id，赋值给结果表
    conf_id = get_conf_id(news_heading)

    dict_academic = dict()
    dict_academic = {'网站名称': spider_name, '网站域名': spider_url}

    # 创建文件夹
    # 先判断文件夹是否存在，不存在则创建文件夹
    now_dir = os.getcwd()
    new_dir = now_dir + '\\' + news_heading
    dir_judge = os.path.exists(new_dir)
    if not dir_judge:
        os.mkdir(new_dir)

    # 每一页的url
    for url in url_list:

        r = requests.get(url, headers=headers)
        r.encoding = 'UTF-8'
        raw_html = r.text
        html = etree.HTML(raw_html)

        # 筛选处理讲座链接
        links_list = get_xpath_content(html, '学术预告网址xpath')
        temp = []
        for each in links_list:
            if 'http' in each:
                temp.append(each)
        links_list = temp

        title_list = get_xpath_content(html, '学术预告标题xpath')

        # 每一条讲座的url + 每一个标题
        for each_url, title in zip(links_list, title_list):
            print('正在爬取 {} 栏目下，第 {} 页 总第 {} 条讲座。'.format(news_heading, page, sum_i + 1))
            # 存储每一个学术预告链接URL的记录，放进字典和数据库，如果已经存在，则不存储
            judge = each_url in dict_data.keys()
            try:
                if not judge:
                    dict_data[each_url] = title
                    r = requests.get(each_url, headers=headers)
                    r.encoding = 'UTF-8'
                    raw_html = r.text
                    html = etree.HTML(raw_html)

                    html_filter = sensitive_word_filter(raw_html)
                    html_filter = path_rewrite(html_filter)
                    timestamp = round(time.time())
                    html_file = new_dir + '\\' + str(timestamp) + '.html'
                    pdf_file = new_dir + '\\' + str(timestamp) + '.pdf'

                    dict_academic['所属栏目'] = news_heading

                    # 从数据库获取xpath, 并根据xpath获取内容
                    try:
                        cur.execute("SELECT name from t_spider_config_xpath where name like %s", news_heading + '%')
                        xpath_name = cur.fetchall()
                        for each in xpath_name:
                            # [4:-5]表示去掉开头‘学术预告’四个字符和结尾‘xpath’五个字符
                            dict_academic[each[0][4:-5]] = get_xpath_content(html, each[0])
                    except IndexError:
                        print("xpath配置错误！")
                    except etree.XPathEvalError:
                        print("数据库里未找到记录！")

                    dict_academic['标题'] = title
                    dict_academic['网址'] = each_url

                    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    dict_academic['采集时间'] = time_now
                    dict_academic['采集人'] = '档案馆'
                    json_dict = json.dumps(dict_academic, ensure_ascii=False, indent=4)
                    print(json_dict)
                    judge_identifier = not_found_judge(raw_html)
                    # 判断网页是不是404 not found
                    if judge_identifier:
                        cur.execute(insert_result, (conf_id, 'detail', each_url, html_filter, html_file, pdf_file,
                                                    time_now, news_heading, None, json_dict))
                        conn.commit()
                        json_data = json.dumps(dict_data)
                        f_data.seek(0, 0)
                        f_data.write(json_data)

                        sum_i += 1

                        with open(html_file, 'w+', encoding='UTF-8') as f1:
                            f1.write(html_filter)
                        # html转pdf
                        pdfkit.from_url(each_url, pdf_file, configuration=confg)
                        print('该讲座预告《{}》pdf格式已转换成功。'.format(title))
                        time.sleep(sleep_time)
                    else:
                        # 将404 not found 记录进数据库
                        html_filter = '404 not found'
                        cur.execute(insert_result, (conf_id, 'detail', each_url, html_filter, '', '',
                                                    time_now, news_heading, None, json_dict))
                        conn.commit()
                        json_data = json.dumps(dict_data)
                        f_data.seek(0, 0)
                        f_data.write(json_data)
                        print('该讲座预告《{}》网页不存在， 以‘404 not found’为网页内容存入数据库。'.format(title))
                        sum_i += 1
                else:
                    sum_i += 1
                    print('{} 栏目 的 第 {} 条讲座预告 已爬取过且保存在数据库中！'.format(news_heading, sum_i))
            except IOError:
                print("Warning: wkhtmltopdf读取文件失败, 可能是网页无法打开或者图片/css样式丢失。")
            except IndexError:
                print("该栏目《{}》下的讲座预告已全部爬取完！".format(news_heading))
                break

        print('第{}页已经爬取完'.format(page))
        page += 1

    print('{} 栏目下 共有{}页 {}条讲座预告'.format(news_heading, page - 1, sum_i))


# 读取快讯每个页面的url，获取快讯的每条新闻的归档元数据，并将页面转成pdf格式保存
def get_express_info(url_list, f_data):
    global dict_data
    # 读取字典中的数据
    f_data.seek(0, 0)
    content = f_data.read()
    if content:
        dict_data = json.loads(content)


    # 快讯新闻数累加器
    sum_i = 0

    # 快讯新闻页数计数器
    page = 1

    # 快讯发布时间处理计数器
    i = 0

    news_heading = '快讯'

    # 获取配置表的id，赋值给结果表
    conf_id = get_conf_id(news_heading)

    dict_express = dict()
    dict_express = {'网站名称': spider_name, '网站域名': spider_url}

    # 创建文件夹
    # 先判断文件夹是否存在，不存在则创建文件夹
    now_dir = os.getcwd()
    new_dir = now_dir + '\\' + news_heading
    dir_judge = os.path.exists(new_dir)
    if not dir_judge:
        os.mkdir(new_dir)

    for url in url_list:
        # 存储每一个快讯链接URL的记录，放进数据库，如果已经存在，则不存储
        judge = url in dict_data.keys()
        try:
            if not judge:
                # 存储快讯链接及页数名
                express_title = '快讯第' + str(page) + '页'
                dict_data[url] = express_title
                json_data = json.dumps(dict_data)
                f_data.seek(0, 0)
                f_data.write(json_data)

                r = requests.get(url, headers=headers)
                r.encoding = 'UTF-8'
                raw_html = r.text
                html = etree.HTML(raw_html)

                html_filter = sensitive_word_filter(raw_html)
                timestamp = round(time.time())
                html_file = new_dir + '\\' + str(timestamp) + '.html'
                pdf_file = new_dir + '\\' + str(timestamp) + '.pdf'

                # 解析快讯发布时间，标题，内容
                release_time_list, title_list, content_list = [], [], []

                try:
                    cur.execute("SELECT name from t_spider_config_xpath where name like %s", '快讯' + '%')
                    xpath_name = cur.fetchall()
                    for each in xpath_name:
                        # [2:-5]表示去掉开头‘快讯’两个字符和结尾‘xpath’五个字符
                        dict_key = each[0][2:-5]
                        if dict_key == '标题':
                            title_list = get_xpath_content(html, each[0])
                        if dict_key == '发布时间':
                            release_time_list = get_xpath_content(html, each[0])
                        if dict_key == '具体内容':
                            content_list = get_xpath_content(html, each[0])
                        if dict_key != '类栏目标题':
                            dict_express[dict_key] = ''
                except IndexError:
                    print("xpath配置错误！")
                except etree.XPathEvalError:
                    print("数据库里未找到记录！")

                # 格式化发布时间
                temp_list = []
                for each in release_time_list:
                    each = each.strip()
                    # print(each)
                    temp_list.append(each)
                release_time_list = []
                while i < len(temp_list)-1:
                    release_time = temp_list[i] + '月' + temp_list[i+1] + '日'
                    release_time_list.append(release_time)
                    i += 2
                # 将计数器清零
                i = 0

                # 格式化快讯内容
                temp_list = []
                for each in content_list:
                    each = each.strip()
                    temp_list.append(each)
                content_list = temp_list

                for release_time, title, content in zip(release_time_list, title_list, content_list):
                    print('正在爬取 {} 栏目下，第 {} 页 总第 {} 条快讯。'.format(news_heading, page, sum_i + 1))
                    print('发布时间：{}, 快讯标题：{}, 快讯内容：{}'.format(release_time, title, content))

                    # 更新字典，并转成json格式
                    dict_express['所属栏目'] = news_heading
                    dict_express['标题'] = title
                    dict_express['发布时间'] = release_time
                    dict_express['具体内容'] = content
                    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    dict_express['采集时间'] = time_now
                    dict_express['采集人'] = '档案馆'
                    json_dict = json.dumps(dict_express, ensure_ascii=False, indent=4)
                    print(json_dict)
                    cur.execute(insert_result, (conf_id, 'detail', url, html_filter, html_file, pdf_file,
                                                time_now, news_heading, None, json_dict))
                    conn.commit()

                    sum_i += 1

                with open(html_file, 'w+', encoding='UTF-8') as f1:
                    f1.write(html_filter)
                # html转pdf
                pdfkit.from_string(html_filter, pdf_file, configuration=confg)
                print('快讯第 {} 页pdf格式已转换成功。'.format(page))
                time.sleep(sleep_time)
            else:
                print('{} 栏目 第{}页快讯 已爬取过且保存在数据库中！'.format(news_heading, page))
        except IOError:
            print("Warning: wkhtmltopdf读取文件失败, 可能是网页无法打开或者图片/css样式丢失。")
        except IndexError:
            print("该栏目《{}》下的新闻已全部爬取完！".format(news_heading))
            break

        print('第{}页已经爬取完'.format(page))
        page += 1

    print('{} 栏目下 共有{}页 {}条快讯'.format(news_heading, page - 1, sum_i))


# 获取专题的各个详细页面html，并转成pdf格式保存
def get_topic_info(url_dict, f_data):
    global dict_data
    # 读取字典中的数据
    f_data.seek(0, 0)
    content = f_data.read()
    if content:
        dict_data = json.loads(content)


    # 专题数累加器
    sum_i = 0

    news_heading = '专题'

    # 获取配置表的id，赋值给结果表
    conf_id = get_conf_id(news_heading)

    dict_topic = dict()
    dict_topic = {'网站名称': spider_name, '网站域名': spider_url}

    # 创建文件夹
    # 先判断文件夹是否存在，不存在则创建文件夹
    now_dir = os.getcwd()
    new_dir = now_dir + '\\' + news_heading
    dir_judge = os.path.exists(new_dir)
    if not dir_judge:
        os.mkdir(new_dir)

    # key: 专题标题， value：专题链接
    for key, value in url_dict.items():
        print('正在爬取 {} 栏目下，第 {} 个专题。'.format(news_heading, sum_i + 1))
        # 存储每一个专题链接URL的记录，放进字典和数据库，如果已经存在，则不存储
        judge = value in dict_data.keys()
        try:
            if not judge:
                dict_data[value] = key
                res = requests.get(value, headers=headers)
                res.encoding = 'UTF-8'
                raw_html = res.text
                # 判断网页是不是‘404 not found’
                judge_identifier = not_found_judge(raw_html)
                if judge_identifier:

                    html_filter = sensitive_word_filter(raw_html)
                    timestamp = round(time.time())
                    html_file = new_dir + '\\' + str(timestamp) + '.html'
                    pdf_file = new_dir + '\\' + str(timestamp) + '.pdf'

                    # 更新字典，并转成json格式
                    dict_topic['所属栏目'] = news_heading
                    dict_topic['标题'] = key
                    dict_topic['网址'] = value
                    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    dict_topic['采集时间'] = time_now
                    json_dict = json.dumps(dict_topic, ensure_ascii=False, indent=4)
                    print(json_dict)
                    cur.execute(insert_result, (conf_id, 'detail', value, html_filter, html_file, pdf_file,
                                                time_now, news_heading, None, json_dict))
                    json_data = json.dumps(dict_data)
                    f_data.seek(0, 0)
                    f_data.write(json_data)
                    conn.commit()

                    with open(html_file, 'w+', encoding='UTF-8') as f1:
                        f1.write(html_filter)
                    # html转pdf
                    pdfkit.from_url(value, pdf_file, configuration=confg)
                    print('该专题《{}》pdf格式已转换成功。'.format(key))
                    time.sleep(sleep_time)

                else:
                    # 将404 not found 记录进数据库
                    html_filter = '404 not found'
                    time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cur.execute(insert_result,
                                (conf_id, 'detail', value, html_filter, '', '', time_now, news_heading, None, ''))
                    conn.commit()
                    json_data = json.dumps(dict_data)
                    f_data.seek(0, 0)
                    f_data.write(json_data)
                    print('该专题《{}》网页不存在， 以‘404 not found’为网页内容存入数据库。'.format(key))
            else:
                print('{} 栏目 {} 专题已爬取过且保存在数据库中！'.format(news_heading, key))
        except IOError:
            print("Warning: wkhtmltopdf读取文件失败, 可能是网页无法打开或者图片/css样式丢失。")
        except IndexError:
            print("该栏目《{}》下的新闻已全部爬取完！".format(news_heading))
            break

        sum_i += 1

    print('{} 栏目下 共有{}条专题'.format(news_heading, sum_i))


# 判断网页是否是404_not_found, 并返回一个判断标识, 0为空网页，1为正常网页
def not_found_judge(html, r=None):
    judge_identifier = 1
    # temp/temp_2,3,4 找到'404 not found'/'页面不存在'返回下标，找不到为-1
    # 如果网页编码为gb2312，则对网页重新编码解析
    if r:
        encode_judge = html.find('gb2312')
        if encode_judge:
            r.encoding = 'gb2312'
            html = r.text

    temp = html.find('404 Not Found')
    temp_2 = html.find('页面不存在')
    temp_3 = html.find('页面未找到')
    temp_4 = html.find('Page Not Found')
    temp_5 = html.find('<div class="content guery" style="display:inline-block;display:-moz-inline-stack;zoom:1;*display:inline; max-width:280px">')

    if temp != -1 or temp_2 != -1 or temp_3 != -1 or temp_4 != -1 or temp_5 != -1:
        judge_identifier = 0
        print('该网页目前无法访问！')
    return judge_identifier


# 敏感词过滤
def sensitive_word_filter(content):
    ah = Ac_auto.ac_automation()
    path = 'sensitive_words.txt'
    ah.parse(path)
    content = ah.words_replace(content)
    # text1 = "新疆骚乱苹果新品发布会"
    # text2 = ah.words_replace(text1)
    # print(text1)
    # print(text2)

    return content


# 网页图片相对路径转绝对路径
# 读入一个html原码，修正图片路径后return一个新的html代码
def path_rewrite(html):
    new_html = re.sub('="/uploadfile/', '="http://news.cqu.edu.cn/uploadfile/', html)

    return new_html


def main():
    """
    获取所有的栏目链接
    all_news_urls[0-4]: 爬取的第一大类：新闻模块（包括综合新闻、教学科研、招生就业、交流合作、校园生活栏目）
    all_news_urls[5]:爬取的第二大类：媒体重大
    all_news_urls[6]:爬取的第三大类：通知公告简报
    all_news_urls[7]:爬取的第四大类：学术预告
    all_news_urls[8]:爬取的第五大类：快讯
    all_news_urls[9]:爬取的第六大类：专题
    """
    with open('dict_data.txt', 'r+') as f_data:
        all_news_urls = all_urls_list(f_data)

        # 获取每个栏目下每页的链接
        # 爬取的第一大类：新闻模块（包括综合新闻、教学科研、招生就业、交流合作、校园生活栏目）
        for url in all_news_urls[:5]:
            url_list = get_url_list(url, all_news_urls, f_data)
            url_list = [url_list[2]]
            get_news_info(url_list, url, all_news_urls, f_data)
            time.sleep(sleep_time)

        # 爬取的第二大类：媒体重大
        url = all_news_urls[5]
        url_list = get_url_list(url, all_news_urls, f_data)
        url_list = [url_list[2]]
        get_media_info(url_list, f_data)

        time.sleep(sleep_time)

        # 爬取的第三大类：通知公告简报
        url = all_news_urls[6]
        url_list = get_url_list(url, all_news_urls, f_data)
        url_list = [url_list[2]]
        get_notice_info(url_list, f_data)

        time.sleep(sleep_time)

        # 爬取的第四大类：学术预告
        url = all_news_urls[7]
        url_list = get_url_list(url, all_news_urls, f_data)
        url_list = [url_list[2]]
        get_academic_info(url_list, f_data)

        time.sleep(sleep_time)

        # 爬取的第五大类：快讯
        url = all_news_urls[8]
        url_list = get_url_list(url, all_news_urls, f_data)
        url_list = [url_list[2]]
        get_express_info(url_list, f_data)

        time.sleep(sleep_time)

        # 爬取的第六大类：专题。
        url = all_news_urls[9]
        url_dict = get_topic_url_list(url, f_data)
        get_topic_info(url_dict, f_data)

    time.sleep(sleep_time)

    print('{}的爬虫任务已完成！'.format(spider_url))


cur = conn.cursor()

if __name__ == '__main__':
    main()
    # 爬虫结束，更新爬虫状态为-1，停止
    cur.execute("UPDATE t_spider_task SET status = -1 WHERE id = %s", task_id)
    cur.close()
    conn.commit()
    conn.close()
