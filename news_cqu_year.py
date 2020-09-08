"""
爬取原网页的html，过滤新闻内容并重新拼接，保留原网页样式。
"""

from news_cqu import *
import sys

# 睡眠时间
sleep_time = 0.1

# 全局字典变量，以键值对（键：对应URL，值：标题）形式存储爬取的数据记录。
dict_data = dict()


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
    new_dir = now_dir + '/' + news_heading
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
                    html_file = new_dir + '/' + str(timestamp) + '.html'
                    pdf_file = new_dir + '/' + str(timestamp) + '.pdf'

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
                    # print(json_dict)

                    if release_time:
                        date_release_time = datetime.datetime.strptime(release_time, '%Y-%m-%d').date()
                        if news_timefilter == date_release_time.year:
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
                        elif date_release_time.year == news_timefilter - 1:
                            print('该新闻发布时间早于 {} 年，已退出。'.format(news_timefilter))
                            break
                        # 发布时间不是指定年份的新闻则跳过
                        else:
                            print('该新闻发布时间晚于 {} 年，已跳过。'.format(news_timefilter))
                            sum_i += 1
                    # 忽略发布时间的新闻
                    else:
                        print('该新闻没有发布时间，已跳过。')
                        sum_i += 1
                else:
                    sum_i += 1
                    print('{} 栏目 的 第 {} 条新闻 已爬取过且保存在数据库中！'.format(news_heading, sum_i))
            except IOError:
                print("Warning: wkhtmltopdf读取文件失败, 可能是网页无法打开或者图片/css样式丢失。")
            except IndexError:
                print("该栏目《{}》下的新闻已全部爬取完！".format(news_heading))
                break

        # 跳出双重循环
        else:
            print('第{}页已经爬取完'.format(page))
            page += 1
            continue
        break

    print("该栏目《{}》下 {} 年的所有新闻已全部爬取完！".format(news_heading, news_timefilter))
    # print('{} 栏目下 共有{}页 {}条新闻'.format(news_heading, page - 1, sum_i))


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
    new_dir = now_dir + '/' + news_heading
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
                    html_file = new_dir + '/' + str(timestamp) + '.html'
                    pdf_file = new_dir + '/' + str(timestamp) + '.pdf'

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
                    # print(json_dict)

                    if release_time:
                        date_release_time = datetime.datetime.strptime(release_time, '%Y-%m-%d').date()
                        if news_timefilter == date_release_time.year:
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

                        elif date_release_time.year == news_timefilter - 1:
                            print('该新闻发布时间早于 {} 年，已退出。'.format(news_timefilter))
                            break
                        # 发布时间不是指定年份的新闻则跳过
                        else:
                            print('该新闻不是发布于 {} 年的新闻，已跳过。'.format(news_timefilter))
                            sum_i += 1
                    # 忽略没有发布时间的新闻
                    else:
                        print('该新闻没有发布时间，已跳过。')
                        sum_i += 1
                else:
                    sum_i += 1
                    print('{} 栏目 的 第 {} 条通知 已爬取过且保存在数据库中！'.format(news_heading, sum_i))
            except IOError:
                print("Warning: wkhtmltopdf读取文件失败, 可能是网页无法打开或者图片/css样式丢失。")
            except IndexError:
                print("该栏目《{}》下的通知公告简报已全部爬取完！".format(news_heading))
                break

        # 跳出双重循环
        else:
            print('第{}页已经爬取完'.format(page))
            page += 1
            continue
        break

    print("该栏目《{}》下 {} 年的所有新闻已全部爬取完！".format(news_heading, news_timefilter))
    # print('{} 栏目下 共有{}页 {}条通知公告简报'.format(news_heading, page - 1, sum_i))


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
            get_news_info(url_list, url, all_news_urls, f_data)
            time.sleep(sleep_time)

        # 爬取的第三大类：通知公告简报
        url = all_news_urls[6]
        url_list = get_url_list(url, all_news_urls, f_data)
        get_notice_info(url_list, f_data)

        time.sleep(sleep_time)

    print('{}  {} 的 {} 年的新闻 爬虫任务已完成！'.format(spider_name, spider_url, news_timefilter))


if __name__ == '__main__':
    news_timefilter = 0
    try:
        if sys.argv[1]:
            # 爬取新闻的指定日期
            news_timefilter = int(sys.argv[1])
    except IndexError:
        print('未使用年份参数！')
        print('请使用指令python3 news_cqu_year.py [指定年份] 传入年份参数！')

    if news_timefilter:
        main()
        # 爬虫结束，更新爬虫状态为-1，停止
        cur.execute("UPDATE t_spider_task SET status = -1 WHERE id = %s", task_id)
        cur.close()
        conn.commit()
        conn.close()

