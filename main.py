import re
import time
from selenium.webdriver import DesiredCapabilities
import lxml
import config
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pymysql
import config


class getStatistic:
    def __init__(self):
        print("init statistic")
        self.url = "https://www.wildberries.ru/brands/" + config.name + "?page={}"
        self.driver = webdriver.Chrome(options=self.set_chrome_options())
        #self.driver = webdriver.Chrome(ChromeDriverManager().install())
        print("connected to web driver")

    def set_chrome_options(self) -> None:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_prefs = {}
        chrome_options.experimental_options["prefs"] = chrome_prefs
        chrome_prefs["profile.default_content_settings"] = {"images": 2}
        return chrome_options

    def get_search_data_statistic(self):
        print("get search data statistic")
        url = "https://www.wildberries.ru/catalog/0/search.aspx?page={}&sort=popular&search={}"
        statistic = []
        for now_keyword in sqlManager().get_keywords():
            print("get data for keyword: " + now_keyword[0])
            number = 0
            for i in range(1, config.statistic.max_page_for_search_statistic):

                self.driver.get(url.format(i, now_keyword[0]))
                time.sleep(config.statistic.wait_time)
                page = soup = product_cards = 0
                try:
                    page = self.driver.page_source
                    soup = BeautifulSoup(page, "lxml")
                    soup = soup.find('div', class_="product-card-list")
                    product_cards = soup.find_all('a', class_="product-card__main j-card-link")
                except Exception as e:
                    time.sleep(config.statistic.exeption_wait_time)
                    page = self.driver.page_source
                    soup = BeautifulSoup(page, "lxml")
                    soup = soup.find('div', class_="product-card-list")
                    try:
                        product_cards = soup.find_all('a', class_="product-card__main j-card-link")
                    except Exception as e:
                        continue
                for j in product_cards:
                    number += 1
                    index = int(re.findall(r'\d+', j["href"])[0])
                    for k in [int(x[0]) for x in sqlManager().get_index()]:
                        if index == k:
                            statistic.append((index, now_keyword[1], number))

        self.driver.close()
        self.driver.quit()
        return statistic


class sqlManager:
    def __init__(self):
        host = config.sql.host
        user = config.sql.user
        password = config.sql.password
        database = config.sql.database
        port = config.sql.port
        self.db = pymysql.connect(host=host, user=user, password=password, database=database, port=port,
                                  charset='utf8mb4')
        self.cursor = self.db.cursor()

    def get_index(self):
        sql = "SELECT vendorCode FROM product_on_shop"
        self.cursor.execute(sql)
        index = self.cursor.fetchall()
        return index

    def insert(self, products):
        for i in products:
            sql = "INSERT INTO product_on_shop (name, price, vendorCode) VALUES ('{}', '{}', {})".format(str(i[0]),
                                                                                                         i[1],
                                                                                                         i[2]).replace(
                "''", "'")
            self.cursor.execute(sql)
        self.db.commit()
        print("insert successful!!")

    def get_keywords(self):
        sql = "SELECT word, id FROM keywords"
        self.cursor.execute(sql)
        keywords = self.cursor.fetchall()
        return keywords

    def push_today_statistic(self, statistic):
        for i in statistic:
            sql = "INSERT INTO statistic_keywordsday_to_day (dayGet, vendorCode, id_keywords, top) VALUES (NOW(), '{}', '{}', '{}');".format(
                str(i[0]), i[1], i[2])
            self.cursor.execute(sql)
        self.db.commit()
        print("insert successful!!")

    def get_info_about_today(self):
        sql = "select * from statistic_keywordsday_to_day where date(NOW()) = dayGet;"
        self.cursor.execute(sql)
        statistic = self.cursor.fetchall()
        return statistic



print("start")
while True:
    if (sqlManager().get_info_about_today() == ()):
        print("Statistics for today are not found")
        x = getStatistic().get_search_data_statistic()
        sqlManager().push_today_statistic(x)
    else:
        print("Statistics for today have already been collected, standby mode")
        time.sleep(60 * 60)
