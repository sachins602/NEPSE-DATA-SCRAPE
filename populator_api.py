from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import mysql.connector

def top_gainers():
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get("https://www.sharesansar.com/top-gainers")

    even_element = driver.find_elements(By.CLASS_NAME, "even")
    odd_element = driver.find_elements(By.CLASS_NAME, "odd")
    odd_list = []
    even_list = []
    concat_list = []
    for odd_tr, even_tr in zip(odd_element, even_element):
        odd_td = odd_tr.find_elements(By.CSS_SELECTOR, "td")
        even_td = even_tr.find_elements(By.CSS_SELECTOR, "td")
        for odd, even in zip(odd_td,even_td):
           o = odd.text
           e = even.text
           odd_list.append(o)
           even_list.append(e)
    final_odd_list = [tuple(odd_list[i:i+6]) for i in range(0, len(odd_list), 6)]
    final_even_list = [tuple(even_list[i:i + 6]) for i in range(0, len(odd_list), 6)]
    for fo,fe in zip(final_odd_list,final_even_list):
        concat_list.append(fo)
        concat_list.append(fe)
    db = mysql.connector.connect(host='localhost', user='root', password='', database="stock")
    cursor = db.cursor()
    sql_query = "INSERT INTO top_gainers VALUES(%s,%s,%s,%s,%s,%s)"
    cursor.executemany(sql_query,concat_list)
    db.commit()
    print("top gainers data inserted to the database")
    cursor.close()
    db.close()


def top_losers():
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get("https://www.sharesansar.com/top-losers")

    even_element = driver.find_elements(By.CLASS_NAME, "even")
    odd_element = driver.find_elements(By.CLASS_NAME, "odd")
    odd_list = []
    even_list = []
    concat_list = []
    for odd_tr,even_tr in zip(odd_element,even_element):
        odd_td = odd_tr.find_elements(By.CSS_SELECTOR,"td")
        even_td = even_tr.find_elements(By.CSS_SELECTOR,"td")
        for odd, even in zip(odd_td, even_td):
           o = odd.text
           e = even.text
           odd_list.append(o)
           even_list.append(e)
    final_odd_list = [tuple(odd_list[i:i+6]) for i in range(0, len(odd_list), 6)]
    final_even_list = [tuple(even_list[i:i + 6]) for i in range(0, len(odd_list), 6)]
    for fo, fe in zip(final_odd_list, final_even_list):
        concat_list.append(fo)
        concat_list.append(fe)
    db = mysql.connector.connect(host='localhost', user='root', password='', database="stock")
    cursor = db.cursor()
    sql_query = "INSERT INTO top_losers VALUES(%s,%s,%s,%s,%s,%s)"
    cursor.executemany(sql_query,concat_list)
    db.commit()
    print("top losers data inserted to the database")
    cursor.close()
    db.close()

def sub_indices():
    url = 'https://www.sharesansar.com/market'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')
    data = soup.find_all('table', class_='table table-bordered table-striped table-hover')[3:4]
    tr_list = []
    header = ['Sub-Indices', 'Turnover', 'Close', 'Point', '% Change']
    for d in data:
        for tr in d.find_all('tr'):
            for td in tr.find_all('td'):
                i_td = td.text.strip()
                tr_list.append(i_td)
    tr_list = [tuple(tr_list[x:x+5]) for x in range(0, len(tr_list), 5)]
    db = mysql.connector.connect(host='localhost', user='root', password='', database="stock")
    cursor = db.cursor()
    sql_query = "INSERT INTO sub_indices VALUES(%s,%s,%s,%s,%s)"
    cursor.executemany(sql_query, tr_list)
    db.commit()
    print("sub_indices data inserted to the database")
    cursor.close()
    db.close()


if __name__=="__main__":
        sub_indices()
        top_gainers()
        top_losers()






