import requests
import json
import sqlite3
import psycopg2
from random import choice
import pandas as pd
from tqdm import tqdm

"""
Create by american_alex
"""


def get_catalogs_wb(target):
    """получение каталога вб"""
    url = 'https://www.wildberries.ru/webapi/menu/main-menu-ru-ru.json'
    headers = {'Accept': "*/*", 'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    response = requests.get(url, headers=headers).json()

    with open('wb_catalogs_data.json', 'w', encoding='UTF-8') as file:
        json.dump(response, file, indent=2, ensure_ascii=False)
        print(f'Данные сохранены в wb_catalogs_data_sample.json')
    data_list = []
    for d in response:
        try:
            for child in d['childs']:
                if target == child['url']:
                    data_list.append({
                        'category_name': child['name'],
                        'category_url': child['url'],
                        'shard': child['shard'],
                        'query': child['query']})
                else:
                    for sub_child in child['childs']:
                        data_list.append({
                            'category_name': sub_child['name'],
                            'category_url': sub_child['url'],
                            'shard': sub_child['shard'],
                            'query': sub_child['query']})
        except:
            # print(f'не имеет дочерних каталогов *{d["name"]}*')
            continue

    return data_list


def search_category_in_catalog(target, catalog_list):
    """пишем проверку пользовательской ссылки на наличии в каталоге"""
    try:
        for catalog in catalog_list:
            if catalog['category_url'] == target:
                print(f'найдено совпадение: {catalog["category_name"]}')
                name_category = catalog['category_name']
                shard = catalog['shard']
                query = catalog['query']
                return name_category, shard, query
            else:
                # print('нет совпадения')
                pass
    except:
        print('Данный раздел не найден!')


def get_data_from_json(json_file):
    """извлекаем из json интересующие нас данные"""

    # Сохранение json для характеристик товара
    # todo переместить сохранение json в get_content
    # with open('wb_data.json', 'w', encoding='UTF-8') as file:
    #     json.dump(json_file, file, indent=2, ensure_ascii=False)
    #     print(f'Данные сохранены в wb_catalogs_data_sample.json')

    data_list = []
    for data in json_file['data']['products']:
        # try:
        #     price = int(data["priceU"] / 100)
        # except:
        #     price = 0
        # print (data)
        try:
            datatime2 = data['time2']
        except KeyError:
            datatime2 = f'-1'

        try:
            datatime1 = data['time1']
        except KeyError:
            datatime1 = f'-1'

        try:
            datadist = data['dist']
        except KeyError:
            datadist = f'-1'

        try:
            datapanelPromoId = data['panelPromoId']
            datapromoTextCat = data['promoTextCat']
        except KeyError:
            datapanelPromoId = f'-1'
            datapromoTextCat = f'-1'

        try:
            datacolorsName = data['colors'][0]['name']
            datacolorsid = data['colors'][0]['id']
        except IndexError:
            datacolorsName = f'-1'
            datacolorsid = f'-1'

        data_list.append({
                '__sort': data['__sort'],
                'time1': datatime1,
                'time2': datatime2,
                'dist': datadist,
                'id': data['id'],
                'root': data['root'],
                'kindId': data['kindId'],
                'subjectId': data['subjectId'],
                'subjectParentId': data['subjectParentId'],
                'Наименование': data['name'],
                'Бренд': data['brand'],
                'id бренда': data['brandId'],
                'siteBrandId': data['siteBrandId'],
                'supplierId': data['supplierId'],
                'Скидка': data['sale'],
                'priceU': data['priceU'],
                'salePriceU': data['salePriceU'],
                'logisticsCost': data['logisticsCost'],
                'saleConditions': data['saleConditions'],
                'pics': data['pics'],
                'rating': data['rating'],
                'feedbacks': data['feedbacks'],
                'panelPromoId': datapanelPromoId,
                'promoTextCat': datapromoTextCat,
                'volume': data['volume'],
                'diffPrice': data['diffPrice'],

                'colors_name': datacolorsName,
                'colors_id': datacolorsid,

                'sizes_name': data["sizes"][0]["name"],
                'sizes_origName': data['sizes'][0]['origName'],
                'sizes_rank': data['sizes'][0]['rank'],
                'sizes_optionId': data['sizes'][0]['optionId'],
                'sizes_wh': data['sizes'][0]['wh'],
                'sizes_sign': data['sizes'][0]['sign'],

                'Ссылка': f'https://www.wildberries.ru/catalog/{data["id"]}/detail.aspx?targetUrl=BP'
        })
    return data_list


def get_content(shard, query, low_price=None, top_price=None):
    # вставляем ценовые рамки для уменьшения выдачи, вилбериес отдает только 100 страниц
    # headers = {'Accept': "*/*", 'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    desktop_agents = [
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14',
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0']

    headers = {'User-Agent': choice(desktop_agents), 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'}

    data_list = []
    for page in tqdm(range(1, 101)):

        # url = f'https://wbxcatalog-ru.wildberries.ru/{shard}' \
        #       f'/catalog?appType=1&curr=rub&dest=-1029256,-102269,-1278703,-1255563' \
        #       f'&{query}&lang=ru&locale=ru&sort=sale&page={page}' \
        #       f'&priceU={low_price * 100};{top_price * 100}'
        url = f'https://catalog.wb.ru/catalog/{shard}/catalog?appType=1&curr=rub&dest=-1075831,-77677,-398551,12358499' \
              f'&locale=ru&page={page}&priceU={1 * 100};{1000000 * 100}' \
              f'&reg=0&regions=64,83,4,38,80,33,70,82,86,30,69,1,48,22,66,31,40&sort=popular&spp=0&{query}'
        data = requests.get(url, headers=headers).json()
        if len(get_data_from_json(data)) > 0:
            data_list.extend(get_data_from_json(data))
        else:
            print(f'Сбор данных завершен.')
            break
    # print (data_list)
    return data_list


def save_excel(data, filename):
    """сохранение результата в excel файл"""
    df = pd.DataFrame(data)
    writer = pd.ExcelWriter(f'{filename}.xlsx')
    df.to_excel(writer, 'data')
    writer.save()
    print(f'Все сохранено в {filename}.xlsx')


def save_database(data_list, conSQLite, conPostgreSQL):
    """Загрузка Pandas-таблицы wb_points в SQLite и PostgreSQL"""
    data = pd.DataFrame(data_list)
    data = list(data.itertuples(index=True, name=None))
    conSQLite.cursor().executemany(
        "INSERT into wb_products values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        data)
    conSQLite.commit()
    conPostgreSQL.cursor().executemany(
        "INSERT into wb_products values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        data)
    conPostgreSQL.commit()


def parser_all(url, low_price, top_price, conSQLite, conPostgreSQL):
    """парсим все подряд"""
    # todo нужно добавить перебор каталогов
    # получаем список каталогов
    target = url
    catalog_list = get_catalogs_wb(target)
    try:
        # поиск введенной категории в общем каталоге
        name_category, shard, query = search_category_in_catalog(target, catalog_list=catalog_list)
        # сбор данных в найденном каталоге
        data_list = get_content(shard=shard, query=query, low_price=low_price, top_price=top_price)
        # сохранение найденных данных
        #save_excel(data_list, f'{name_category}_from_{low_price}_to_{top_price}')
        save_database(data_list, conSQLite, conPostgreSQL)
    except TypeError:
        print('Ошибка! Возможно не верно указан раздел. Удалите все доп фильтры с ссылки')
    except PermissionError:
        print('Ошибка! Вы забыли закрыть созданный ранее excel файл. Закройте и повторите попытку')

# def parser(url, low_price, top_price, conSQLite, conPostgreSQL):
#     """парсим товары по ссылке"""
#     # получаем список каталогов
#     target = url.split('https://www.wildberries.ru')[-1]
#     catalog_list = get_catalogs_wb(target)
#     try:
#         # поиск введенной категории в общем каталоге
#         name_category, shard, query = search_category_in_catalog(target, catalog_list=catalog_list)
#         # сбор данных в найденном каталоге
#         data_list = get_content(shard=shard, query=query, low_price=low_price, top_price=top_price)
#         # сохранение найденных данных
#         save_database(data_list, conSQLite, conPostgreSQL)
#     except TypeError:
#         print('Ошибка! Возможно не верно указан раздел. Удалите все доп фильтры с ссылки')
#     except PermissionError:
#         print('Ошибка! Вы забыли закрыть созданный ранее excel файл. Закройте и повторите попытку')

if __name__ == '__main__':
    # todo не работает, если указывать ссылку на категорию с фильтрами (цена, сортировка и т.д.)
    # Заполнять следующим образом:
    # url = input('Введите ссылку на категорию для сбора: ')
    # low_price = int(input('Введите минимальную сумму товара: '))
    # top_price = int(input('Введите максимульную сумму товара: '))

    # Подключение к SQLite и PostgreSQL
    conSQLite = sqlite3.connect("PBD_DZ_SQLite.db")
    conPostgreSQL = psycopg2.connect(user="postgres",
                                     password="1234",
                                     host="127.0.0.1",
                                     port="5432",
                                     database="PBD_DZ_PostgreSQL")

    # Создание таблиц в SQLite и PostgreSQL
    Create_wb_products = '''CREATE TABLE IF NOT EXISTS wb_products (
sort INT,
ksort INT,
time1 INT,
time2 INT,
dist INT,
id INT,
root INT,
kindId INT,
subjectId INT,
subjectParentId INT,
name TEXT,
brand TEXT,
brandId INT,
siteBrandId INT,
supplierId INT,
sale INT,
priceU INT,
salePriceU INT,
logisticsCost INT,
saleConditions INT,
pics INT,
rating INT,
feedbacks INT,
panelPromoId INT,
promoTextCat TEXT,
volume INT,
diffPrice TEXT,
colorsName TEXT,
colorsId INT,
sizeName TEXT,
sizesOrigName TEXT,
sizesRank INT,
sizesOptionId INT,
sizesWh INT,
sizesSign TEXT,
link TEXT);'''
    conSQLite.cursor().execute(Create_wb_products)
    conSQLite.commit()
    conPostgreSQL.cursor().execute(Create_wb_products)
    conPostgreSQL.commit()

    """собираем данные по ссылке на каталог"""
    # url = 'https://www.wildberries.ru/catalog/sport/vidy-sporta/velosport/velosipedy'
    # url = 'https://www.wildberries.ru/catalog/zhenshchinam/odezhda/bryuki-i-shorty/shorty'
    low_price = 1
    top_price = 500000
    # parser(url, low_price, top_price)

    """собираем все данные из всех каталогов"""
    with open('wb_catalogs_data.json', 'r', encoding='UTF-8') as json_file:
        data = json.load(json_file)
        data_list = []
        listID = [128313, 128604, 9156, 62057]
        for d in data:
            try:
                if d['id'] not in listID:
                    for child in d['childs']:
                        try:
                            for child1 in child['childs']:
                                try:
                                    if child1['id'] not in listID:
                                        for child2 in child1['childs']:
                                            try:
                                                for child3 in child2['childs']:
                                                    data_list.append(
                                                        child3['url'])
                                            except:
                                                data_list.append(child2['url'])
                                except:
                                    data_list.append(child1['url'])
                        except:
                            data_list.append(child['url'])
            except:
                continue

    for item in data_list:
        print(item)
        parser_all(item, low_price, top_price, conSQLite, conPostgreSQL)
        #break

    # Отключение от SQLite и PostgreSQL
    conSQLite.close()
    conPostgreSQL.close()
