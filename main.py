import datetime
import json
import logging
import os.path
import time
import psycopg2
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm

logging.basicConfig(filename="log.log", level=logging.INFO)


class ConnectDB:

    def __init__(self):
        self.conn = psycopg2.connect(host=os.environ["db_host"],
                                     port=os.environ["db_port"],
                                     database=os.environ["db_database"],
                                     user=os.environ["db_user"],
                                     password=os.environ["db_password"])
        self.cursor = self.conn.cursor()

    def status(self):
        return self.cursor.statusmessage

    def query(self, request, data):
        self.cursor.execute(request, data)
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()


class Cursor:
    def __init__(self):
        self.cursor = ConnectDB()

    def insert(self, request, data):
        self.cursor.query(request, data)
        return self.cursor.status()

    def __del__(self):
        self.cursor.close()


def timer(f):
    def wrap_timer(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        delta = time.time() - start
        print(f'Время выполнения функции {f.__name__} составило {delta} секунд')
        return result

    return wrap_timer


def log(f):
    def wrap_log(*args, **kwargs):
        logging.info(f"Запущена функция {f.__doc__}. Время {time.time()}")
        result = f(*args, **kwargs)
        logging.info(f"Результат: {result}")
        return result

    return wrap_log


@log
@timer
def get_pages(final_page: int, d: dict) -> None:
    """
    :param final_page: page number when the work stops.
    :param d: dict of int region code: <city_name>
    """

    service = ChromeService(executable_path=ChromeDriverManager().install())
    options = ChromeOptions()
    driver = webdriver.Chrome(service=service, options=options)

    driver.maximize_window()

    # k - region code in d
    # v - city_name in d
    for k, v in d.items():
        region = k
        city_name = v

        cian = 'https://cian.ru'  # Коннект к циану, ссылка с параметрами сразу не открывается
        driver.get(cian)
        driver.implicitly_wait(5)

        # Коннект с параметрами, ссылка с пагинацией сразу не открывается
        URL = f'https://{city_name}.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&region={region}&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1'
        driver.get(URL)
        driver.implicitly_wait(5)

        # Коннект с пагинацией
        URLp = f'https://{city_name}.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p=1&region={region}&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1'
        driver.get(URLp)
        driver.implicitly_wait(5)
        URLp = driver.current_url

        count_cities = 1

        try:
            count_pages = 0
            for i in tqdm(range(1, final_page + 1)):

                page = f'https://{city_name}.cian.ru/cat.php?deal_type=sale&engine_version=2&&offer_type=flat&p={i}&region={region}&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&room7=1&room9=1'
                driver.get(page)
                driver.implicitly_wait(10)
                if driver.current_url == URLp:
                    logging.info(
                        f'Invalid page number {i - 1}. Redirect to first page. Programm stops for {city_name} stops in {i - 1}')
                    print(f"Выполнение программы для {city_name} закончилось на {i - 1} странице")

                    break

                # captcha button - find by <div class="bubbles"></div>

                if not os.path.exists(f"pages_test/{city_name}_{region}"):
                    os.mkdir(f"pages_test/{city_name}_{region}")

                with open(f'pages_test/{city_name}_{region}/page_{i}.html', 'w', encoding='utf-8') as file:
                    file.write(driver.page_source)
                logging.info(f"Загружена страница {i}")

            count_pages += 1

        except Exception as e:
            print(e)
            logging.info('Error catched')
            logging.error(e)

        count_cities += 1

    print(f"Обработано {count_cities} населенных пунктов")


def get_data(d: dict) -> None:
    """

    :param d: dict which contains region code and city name
    :return:
    """
    cursor = Cursor()

    for k, v in d.items():
        region = k
        city_name = v
        l = os.listdir(path=f'pages/{city_name}_{region}')

        print(f"{city_name}_{region}")

        for i in range(1, len(l) + 1):
            with open(f"pages/{city_name}_{region}/page_{i}.html", 'r', encoding="utf-8") as file:

                data = file.read()
                soup = BeautifulSoup(data, 'html.parser')
                tag_list = soup.find("body").find_all("script", type="text/javascript")

                s = tag_list[3].text.strip()
                # срез строки скрипта js чтобы достать json
                j = s[136:len(s) - 2]  # заменить

                json_data = json.loads(j)

                # "key": initialState - key of dict which contains data about offers

                if json_data[len(json_data) - 1]["key"] != 'initialState':
                    logging.error('Invalid key in json_data. Needed "initialState".')
                    continue

                offers = json_data[len(json_data) - 1]["value"]["results"]["offers"]
                for j in offers:

                    added_date = j["addedTimestamp"]
                    offer = {
                        "offer_id": j["id"],
                        "added_date": datetime.datetime.fromtimestamp(added_date).strftime("%Y-%m-%d"),
                        "date_parsing": datetime.datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d"),
                        "region": region,
                        "category": j["category"],
                        "flat_type": j["flatType"],
                        "offer_type": j["offerType"],
                        "total_area": j["totalArea"],
                        "price": j["bargainTerms"]["priceRur"],
                        "phone": j["phones"][0]["countryCode"] + j["phones"][0]["number"],
                        "address": j["geo"]["userInput"],
                        "lat": j["geo"]["coordinates"]["lat"],
                        "lng": j["geo"]["coordinates"]["lng"],
                        "link": j["fullUrl"],
                    }

                    # table house

                    house = {
                        "offer_id": j["id"],
                        "year_house": j["building"]["buildYear"],
                        "floor": j["floorNumber"],
                        "floors_count": j["building"]["floorsCount"],
                        "material_type": j["building"]["materialType"]
                    }

                    offer_columns = offer.keys()
                    offer_values = [offer[x] for x in offer_columns]
                    offer_values_str_list = ["%s"] * len(offer_values)
                    offer_values_str = ", ".join(offer_values_str_list)

                    house_columns = house.keys()
                    house_values = [house[x] for x in house_columns]
                    house_values_str_list = ["%s"] * len(house_values)
                    house_values_str = ", ".join(house_values_str_list)


                    offers_statement = """INSERT INTO public.offers
                    VALUES ({offer_values_str})
                    ON CONFLICT (offer_id) DO UPDATE 
                    SET (added_date, date_parsing, price) = (EXCLUDED.added_date, EXCLUDED.date_parsing, EXCLUDED.price);"""

                    house_statement = """INSERT INTO public.house
                    VALUES ({house_values_str})
                    ON CONFLICT (offer_id) DO UPDATE 
                    SET (year_house, material_type) = (EXCLUDED.year_house, EXCLUDED.material_type)"""

                    cursor.insert(offers_statement.format(offer_values_str=offer_values_str), offer_values)
                    cursor.insert(house_statement.format(house_values_str=house_values_str), house_values)


d = {
    2: "spb",
    4897: "novosibirsk"
}

if __name__ == '__main__':
    get_pages(final_page=60, d=d)
    get_data(d)
