import re
import json
import time
import pymysql
import random
import requests
import urllib.parse
from tianmao.get_proxy_ip import get_proxy
# from mysql.main import python_sql_mysql #自己封装的跳板机MYSQL接口
from pymysql.converters import escape_string


def crawler_bk_con(db_name):
    """
    获取数据库的连接
    :param db_name: 数据库名称
    :return: 连接的游标
    """
    conn = pymysql.connect(host="127.0.0.1",
                           port=3306,
                           user="root",
                           password="12345",
                           database=db_name,
                           charset="utf8mb4")
    return conn


def python_sql_mysql(db_name, sql, is_return=False):
    """
   接受查询语句,执行相对应的sql代码,判断是否需要返回
   :param db_name: 数据库的名称
   :param sql: 需要执行的sql语句
   :param is_return: 此函数是否需要返回值的条件,默认为False
   :return: 如果需要返回值则返回查询结果,如果不需要返回值,则不返回
   """
    conn = crawler_bk_con(db_name=db_name)  # 获取连接游标
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)  # 执行sql语句
            if is_return:  # 判断是否返回,如果为True对齐进行查询操作,如果为False则进行插入操作
                res = cursor.fetchall()
                return res
            else:
                conn.commit()


# 二级品类对应一级品类字典值
dict_1 = {'女装': '男女服饰', '新品': '内衣配饰', '裙子': '男女服饰', '男装': '男女服饰', 'T恤': '男女服饰',
          '休闲': '女鞋市场', '女包': '箱包市场', '男包': '箱包市场', '双肩包': '箱包市场',
          '旅行箱': '箱包市场', '钱包': '箱包市场', '靴子': '女鞋市场', '单鞋': '女鞋市场',
          '男鞋': '女鞋市场', '商务': '女鞋市场', '帆布': '女鞋市场', '文胸': '内衣配饰', '睡衣': '内衣配饰',
          '内裤': '内衣配饰', '帽子': '内衣配饰', '皮带': '内衣配饰', '跑步鞋': '运动市场',
          '球鞋': '运动市场', '运动夹克': '运动市场', '运动套装': '运动市场', '山地车': '户外市场',
          '垂钓用品': '户外市场', '广场舞': '户外市场', '冲锋衣': '户外市场', '轮滑': '户外市场',
          '饰品': '配饰市场', '珠宝': '配饰市场', '手表': '配饰市场', '眼镜': '配饰市场',
          'Zippo军刀': '配饰市场', '手机': '数码市场', '平板电脑': '数码市场', '相机/摄像': '数码市场',
          '笔记本': '数码市场', '智能3C': '数码市场', '大家电': '家电市场', '厨房电器': '家电市场',
          '生活电器': '家电市场', '个人护理': '家电市场', '办公耗材': '家电市场', '护肤': '美妆市场',
          '彩妆': '美妆市场', '香氛': '美妆市场', '男士': '美妆市场', '美发': '美妆市场', '功效': '美妆市场',
          '童装': '母婴市场', '童鞋': '母婴市场', '奶粉': '母婴市场', '用品': '母婴市场', '玩具': '母婴市场',
          '孕产': '母婴市场', '四件套': '家居家纺', '窗帘': '家居家纺', '摆件': '家居家纺',
          '花瓶': '家居家纺', '沙发': '家居家纺', '床': '家居家纺', '厨房卫浴': '家装建材',
          '灯饰': '家装建材', '建材': '家装建材', '五金': '家装建材', '瓷砖': '家装建材', '热门': '日用百货',
          '餐具': '日用百货', '饮具': '日用百货', '收纳': '日用百货', '清洁': '日用百货', '日化': '日用百货',
          '行车记录仪': '阿里汽车', '脚垫': '阿里汽车', '安全座椅': '阿里汽车', '轮胎': '阿里汽车',
          '猫狗': '花鸟市场', '水族': '花鸟市场', '奇宠': '花鸟市场', '鲜花': '花鸟市场', '园艺': '花鸟市场',
          '花艺': '花鸟市场', '古筝': '文娱市场', '萨克斯': '文娱市场', '吉它': '文娱市场',
          '电钢琴': '文娱市场', '非洲鼓': '文娱市场', '钻戒': '婚庆市场', '婚纱': '婚庆市场',
          '喜帖': '婚庆市场', '床品': '婚庆市场', '喜糖': '婚庆市场', '蜜月': '婚庆市场', '腔调': '特色市场',
          '中老年': '特色市场'}
# 请求头
headers = {
    'authority': 'shopsearch.taobao.com',
    'method': 'GET',
    'path': '/search?ie=utf8&initiative_id=staobaoz_20220110&js=1&q=%E6%97%97%E8%88%B0%E5%BA%97+%E5%A5%B3%E8%A3%85',
    'scheme': 'https',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'zh-CN,zh;q=0.9',
    'cache-control': 'max-age=0',
    'cookie': 'cna=sNoIGsp/CBECAW8AcZ4x7kSg; t=1c43d38240316bddbaebeabfe997b4d3; tracknick=%5Cu6D2A1605376101; hng=CN%7Czh-CN%7CCNY%7C156; thw=cn; miid=465500155130289337; _tb_token_=e8875ed3ed4a7; cookie2=1e450399ab96a0c650eebdab305a5ee8; _samesite_flag_=true; v=0; xlly_s=1; sgcookie=E1005AXX2WyTGN5dcO8mFMaK7ujBsy53BuSvC1XiRuoZQla%2BY5tvSdVw%2F5QNwfr5q6l6euXUi0sfCYz13JiqpUODdLB%2FxTqNp49EWSSmSqXyYDZd8Uz35YI3fR%2FcQfn3sWxH; unb=2882106121; uc3=id2=UUBQbmZ7%2B5rIHw%3D%3D&lg2=WqG3DMC9VAQiUQ%3D%3D&nk2=2UkIBBKBFxqGIVoz&vt3=F8dCvUzIdC7oyWZc1h8%3D; csg=ce301d73; lgc=%5Cu6D2A1605376101; cancelledSubSites=empty; cookie17=UUBQbmZ7%2B5rIHw%3D%3D; dnk=%5Cu6D2A1605376101; skt=a1064f0e4f8785b3; existShop=MTY0MTc4MDk2Mg%3D%3D; uc4=nk4=0%4026%2FIeExU8M0eVIVcqYukvGzD8PZV7%2BY%3D&id4=0%40U2LC5PJEu%2FC2yKZ48%2B2wW%2F3exr2z; _cc_=Vq8l%2BKCLiw%3D%3D; _l_g_=Ug%3D%3D; sg=11a; _nk_=%5Cu6D2A1605376101; cookie1=BxAcDHTp7uvamUnFmJcJECK5c0CPw60jXcD1ln8EQtw%3D; enc=lGHPmJirkWjOPS0X9OqGbTINo4FEU91kDlWUf%2FLBWOONESeMCkFdIjYSggyPqUXZTN%2FJ9UYz68Cl8efYTguBVg%3D%3D; mt=ci=13_1; _m_h5_tk=5ff7859d8890e260ab166b3d8c6ce214_1641791212885; _m_h5_tk_enc=25e78b894b7a617c41fd941205578aca; uc1=cookie21=UtASsssmeW6lpyd%2BB%2B3t&cookie14=UoewAeK4PT28cg%3D%3D&cookie16=UIHiLt3xCS3yM2h4eKHS9lpEOw%3D%3D&cookie15=W5iHLLyFOGW7aA%3D%3D&pas=0&existShop=false; JSESSIONID=F8D5B44341B2EBDBD17E431597950105; tfstk=cnadBywiKOXhcxcTgkIgVVJSGt5GZ7X-CBMky1u6xALXIA8RiK40kBOGAbMqp1C..; l=eBxJWdJHgkao89MkBOfwourza77OSIRAguPzaNbMiOCPOgCp5sclW6pgNfT9C3GVh6qwR35T8LL0BeYBqIv4n5U68yvNIODmn; isg=BJ-fo4HuAo1jDAb6PpuKuKznLvMpBPOm-K40ejHsO86VwL9COdSD9h2SglC-2Mse',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
}

func = lambda x: x.replace('\'NULL\'', 'NULL')


def get_max_page(raw_html):
    """
    获取实际的最大店铺数量和实际最大页数(不是100)
    :param raw_html: 原始的html信息
    :return: 返回最大的店铺数量和实际最大页数
    """
    pattern = re.compile(r'g_page_config = (.*?);\n    g_srp_loadCss',
                         flags=re.DOTALL)  # 构建正则表达式
    json_body = pattern.findall(raw_html)
    print(json_body)
    response_content_json = json.loads(json_body[0])
    try:
        totalcount = int(
            response_content_json.get('mods').get('pager').get('data').get(
                'totalCount'))
    except:
        # 针对一些异常数据做的特殊取数方式
        totalcount = int(
            response_content_json.get('mods').get('tab').get('data').get(
                'totalHits'))
    totalpage = int(totalcount // 20)
    totalpage = totalpage + 1
    return totalcount, totalpage


def get_detail_search_html(raw_html, category_name_two, search_key, page):
    """
    解析原始页面,获得详细信息,并且插入数据库
    :param raw_html: 原始页面信息
    :param category_name_two: 二级品类名称
    :param search_key: 搜索名称
    :param page: 此旗舰店属于搜索结果的页数
    :return: 不返回
    """
    pattern = re.compile(r'g_page_config = (.*?);\n    g_srp_loadCss',
                         flags=re.DOTALL)
    json_body = pattern.findall(raw_html)
    response_content_json = json.loads(json_body[0])
    print('response_content_json', response_content_json)
    shop_list = response_content_json.get('mods').get('shoplist').get(
        'data').get('shopItems')
    for shop in shop_list:
        print(shop)
        tb_shop_id = int(shop.get('uid'))
        tb_shop_name = shop.get('title')
        is_tianmao = shop.get('isTmall')
        tb_shop_address = shop.get('provcity', 'NULL')
        if tb_shop_address == '':
            tb_shop_address = 'NULL'
        else:
            pass
        print(tb_shop_id, tb_shop_name, is_tianmao, tb_shop_address)
        # 保存源数据
        python_sql_mysql(db_name=db_name, is_return=False,
                         sql=func(
                             "insert into raw_shop_detail (category_name_one,category_name_two,search_name,"
                             "tb_shop_id,tb_shop_name,is_tianmao,tb_shop_address,page) "
                             "VALUES (\'%s\',\'%s\',\'%s\',%s,\'%s\',%s,\'%s\',%s)" % (
                                 escape_string(dict_1.get(category_name_two)),
                                 escape_string(category_name_two),
                                 escape_string(search_key),
                                 tb_shop_id,
                                 escape_string(tb_shop_name),
                                 is_tianmao, escape_string(tb_shop_address),
                                 page)))
        # 保存没有存在mysql的店铺信息
        if python_sql_mysql(db_name=db_name, is_return=True,
                            sql="select * from shop_detail where tb_shop_id =%s"
                                % (tb_shop_id)):
            pass
        else:
            python_sql_mysql(db_name=db_name, is_return=False,
                             sql=func(
                                 "insert into shop_detail (category_name_one,category_name_two,search_name,"
                                 "tb_shop_id,tb_shop_name,is_tianmao,tb_shop_address,page) "
                                 "VALUES (\'%s\',\'%s\',\'%s\',%s,\'%s\',%s,\'%s\',%s)" % (
                                     escape_string(
                                         dict_1.get(category_name_two)),
                                     escape_string(category_name_two),
                                     escape_string(search_key),
                                     tb_shop_id,
                                     escape_string(tb_shop_name),
                                     is_tianmao,
                                     escape_string(tb_shop_address),
                                     page)))


def search_tao(exis_max_page, max_page, search_key, category_name_two):
    """

    :param exis_max_page: 已经存在的最大页数
    :param max_page: 实际存在的最大页数
    :param search_key: 搜索的关键字
    :param category_name_two: 二级品类名称
    :return: 不返回
    """
    for i in range(exis_max_page + 1, max_page):
        url = 'https://shopsearch.taobao.com/search?ie=utf8&initiative_id=staobaoz_20220111&js=1&q={}&suggest=0_6&_input_charset=utf-8&wq=%E6%97%97%E8%88%B0%E5%BA%97&suggest_query=%E6%97%97%E8%88%B0%E5%BA%97&source=suggest&s={}'.format(
            urllib.parse.quote(search_key), str(i * 20))
        count = 0  # 设置一个请求异常最大次数,超出这个次数跳出
        while True:
            try:
                # 捕获请求超时的异常
                response = requests.get(url=url, headers=headers,
                                        proxies=get_proxy(), timeout=5)
                response.encoding = 'utf-8'
                content = response.text
                # 爬虫数据可能出现请求过快导致返回空数据，需要规避掉这些空数据，
                # 重新发起请求
                # 保存解析好的数据
                get_detail_search_html(raw_html=content,
                                       category_name_two=category_name_two,
                                       search_key=search_key, page=i)
            except:
                count += 1
                if count >= 5:
                    break
                continue
            break
        # 保存原始页面信息
        python_sql_mysql(db_name=db_name, is_return=False,
                         sql="insert into raw_html (category_name_two,search_name,page,raw_html) VALUES (\'%s\',\'%s\',%s,\'%s\')" % (
                             escape_string(category_name_two),
                             escape_string(search_key), i,
                             escape_string(content)))
        # 返回的是真实商品数和可爬取的页数(实际页数远大于可爬取的页数)
        totalcount, totalpage = get_max_page(raw_html=content)

        if python_sql_mysql(db_name=db_name, is_return=True,
                            sql="select * from max_page where search_name=\'%s\'" % (
                                    escape_string(search_key))):
            pass
        else:
            python_sql_mysql(db_name=db_name, is_return=False,
                             sql="insert into max_page (category_name_two,search_name,max_page,max_count) VALUES (\'%s\',\'%s\',%s,%s);" % (
                                 escape_string(category_name_two),
                                 escape_string(search_key), totalpage,
                                 totalcount))
        if totalcount >= 2000:
            max_page = 100
        else:
            max_page = totalpage
        time.sleep(random.randint(3, 5))
        break

    for i in range(exis_max_page + 2, max_page):
        url = 'https://shopsearch.taobao.com/search?ie=utf8&initiative_id=staobaoz_20220111&js=1&q={}&suggest=0_6&_input_charset=utf-8&wq=%E6%97%97%E8%88%B0%E5%BA%97&suggest_query=%E6%97%97%E8%88%B0%E5%BA%97&source=suggest&s={}'.format(
            urllib.parse.quote(search_key), str(i * 20))
        count = 0  # 设置一个请求异常最大次数,超出这个次数跳出
        while True:
            try:
                # 捕获请求超时的异常
                response = requests.get(url=url, headers=headers,
                                        proxies=get_proxy(), timeout=5)
                response.encoding = 'utf-8'
                content = response.text
                # 爬虫数据可能出现请求过快导致返回空数据，需要规避掉这些空数据，
                # 重新发起请求
                # 保存解析好的数据
                get_detail_search_html(raw_html=content,
                                       category_name_two=category_name_two,
                                       search_key=search_key, page=i)
            except:
                count += 1
                if count >= 5:
                    break
                continue
            break
        python_sql_mysql(db_name=db_name, is_return=False,
                         sql="insert into raw_html (category_name_two,search_name,page,raw_html) VALUES (\'%s\',\'%s\',%s,\'%s\')" % (
                             escape_string(category_name_two),
                             escape_string(search_key), i,
                             escape_string(content)))
        time.sleep(random.randint(3, 5))


def main():
    """
    主函数
    :return: 不返回
    """
    select_sql = "select * from tb_category"
    result_list = [i[0] for i in
                   python_sql_mysql(db_name=db_name, sql=select_sql,
                                    is_return=True)]
    key_list = ['官方旗舰店', '旗舰店']
    for result in result_list:
        for key in key_list:
            search_key = result.replace('/', '') + key
            print(search_key)
            if python_sql_mysql(db_name=db_name,
                                sql="select * from max_page where search_name=\'%s\'" % (
                                        search_key), is_return=True):
                # 获取到实际存在的最大页数
                max_page = python_sql_mysql(db_name=db_name,
                                            sql="select max_page from max_page where search_name=\'%s\'" % (
                                                search_key),
                                            is_return=True)[0][0]
                # 获取数据库中存在的最大页数
                exis_max_page = [int(i[0]) for i in
                                 (python_sql_mysql(db_name=db_name,
                                                   sql="select page from raw_html where search_name=\'%s\'" % (
                                                       search_key),
                                                   is_return=True))]
                print(max(exis_max_page))
                if max_page > 100:
                    max_page = 100
                print(max_page)
                search_tao(exis_max_page=max(exis_max_page), max_page=max_page,
                           search_key=search_key, category_name_two=result)
            else:
                print('查询记录不存在')
                search_tao(exis_max_page=-1, max_page=100,
                           search_key=search_key, category_name_two=result)


if __name__ == '__main__':
    db_name = 'crawler_tb'
    main()
