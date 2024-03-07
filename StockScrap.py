import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import datetime
import csv
from kafka import KafkaProducer
import json

# Create a new instance of the Chrome driver
driver = webdriver.Chrome()

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Open a website
driver.get("https://economictimes.indiatimes.com/bajaj-finance-ltd/stocks/companyid-11260.cms")

final_list = []
values_list = []
topic_name = "selenium_tests"


def send_message(topic, message):
    print(message)
    producer.send(topic, json.dumps(message).encode('utf-8'))


def list_data_dump():
    send_message(topic_name, final_list[0])
    final_list.pop(0)


# def csv_data_dump(index):
#     print(final_list[0].values())
#     fieldnames = final_list[0].keys()
#     with open('bajaj_finserv.csv', 'a', newline='') as csvfile:
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         # Check if the file is empty to determine whether to write the header
#         if csvfile.tell() == 0:
#             writer.writeheader()
#         writer.writerows(final_list)
#         send_message(topic_name, final_list[0])
#         final_list.pop(0)
#

def bajaj_finance_data(index):
    # Find an element by its ID and click it
    element = driver.find_element(By.CLASS_NAME, "ltp")
    print(f"Stock Price = {element.text}")

    # FIND THE PE RATIO
    DATA = driver.find_elements(By.CLASS_NAME, 'val')
    index = 0
    print(f"OPEN PRICE = {DATA[2].text}")
    print(f"HIGH = {DATA[3].text}")
    print(f"LOW = {DATA[4].text}")
    print(f"RANK = {DATA[5].text}")
    print(f"PE_RATIO = {DATA[6].text}")
    print(f"EPS = {DATA[7].text}")
    print(f"MCAP = {DATA[8].text}")
    print(f"SECTOR MCAP RANK = {DATA[9].text}")
    print(f"PB RATIO = {DATA[10].text}")
    print(f"Div Yield % = {DATA[11].text}")
    print(f"FACE VALUE = {DATA[12].text}")
    print(f"BETA = {DATA[13].text}")
    print(f"VWAP = {DATA[14].text}")
    print(f"52 Week High/Low = {DATA[15].text}")

    recommend = driver.find_element(By.CLASS_NAME, 'rtype.buy')
    print(f"Analyst Recommendation = {recommend.text}")

    strong_buy_row = driver.find_element(By.CLASS_NAME, 'bg.st_buy.border')
    strong_buy_analyst_value = strong_buy_row.find_elements(By.TAG_NAME, 'td')
    print(f"Strong Buy = {strong_buy_analyst_value[1].text}")

    buy_row = driver.find_element(By.CLASS_NAME, 'buy.border')
    buy_analyst_value = buy_row.find_elements(By.TAG_NAME, 'td')
    print(f"Buy = {buy_analyst_value[1].text}")

    hold = driver.find_element(By.CLASS_NAME, 'bg.hold.border')
    hold_analyst_value = hold.find_elements(By.TAG_NAME, 'td')
    print(f"HOLD = {hold_analyst_value[1].text}")

    sell = driver.find_element(By.CLASS_NAME, 'sell.border')
    sell_analyst_value = sell.find_elements(By.TAG_NAME, 'td')
    print(f"sell = {sell_analyst_value[1].text}")

    strong_sell = driver.find_element(By.CLASS_NAME, 'bg.st_sell.border')
    strong_sell_analyst_value = strong_sell.find_elements(By.TAG_NAME, 'td')
    print(f"Strong Sell = {strong_sell_analyst_value[1].text}")

    current_time = datetime.datetime.now()
    date_str = current_time.strftime("%Y-%m-%d")
    time_str = current_time.strftime("%H:%M:%S")

    # print(date_str)
    # print(time_str)

    final_list.append({
        'Stock Price': element.text,
        "OPEN PRICE": DATA[2].text,
        "HIGH ": DATA[3].text,
        "LOW": DATA[4].text,
        "RANK": DATA[5].text,
        "PE_RATIO": DATA[6].text,
        "EPS": DATA[7].text,
        "MCAP": DATA[8].text,
        "SECTOR MCAP RANK": DATA[9].text,
        "PB RATIO": DATA[10].text,
        "Div Yield percentage": DATA[11].text,
        "FACE VALUE": DATA[12].text,
        "BETA": DATA[13].text,
        "VWAP": DATA[14].text,
        "52 Week High/Low": DATA[15].text,
        "Overall Recommendation": recommend.text,
        "Strong Buy": strong_buy_analyst_value[1].text,
        "Buy": buy_analyst_value[1].text,
        "Hold": hold_analyst_value[1].text,
        "Sell": sell_analyst_value[1].text,
        "Strong_sell": strong_sell_analyst_value[1].text,
        "Date": date_str,
        "Time": time_str
    })
    # values_list.append([
    #     element.text,
    #     DATA[2].text,
    #     DATA[3].text,
    #     DATA[4].text,
    #     DATA[5].text,
    #     DATA[6].text,
    #     DATA[7].text,
    #     DATA[8].text,
    #     DATA[9].text,
    #     DATA[10].text,
    #     DATA[11].text,
    #     DATA[12].text,
    #     DATA[13].text,
    #     DATA[14].text,
    #     DATA[15].text,
    #     recommend.text,
    #     strong_buy_analyst_value[1].text,
    #     buy_analyst_value[1].text,
    #     hold_analyst_value[1].text,
    #     sell_analyst_value[1].text,
    #     strong_sell_analyst_value[1].text,
    #     date_str,
    #     time_str
    # ]
    # )

    # print(final_list)
    # print(values_list)
    print()
    print()


while True:
    index = 0
    bajaj_finance_data(index)
    list_data_dump()
    # csv_data_dump(index)
    index = index + 1
    time.sleep(10)

driver.quit()
