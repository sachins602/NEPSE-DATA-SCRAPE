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



# create table name 'company_details' with 14 columns with datatype varchar(100)
# columns name are 'scrip', 'name', 'sector', 'share_outstanding', 'market_price', 'percentage_change', 'last_traded_on', fiftytwo_week_high_low, onehundredeighty_day_avg, onehundredtwenty_day_avg, one_year_yield, eps, pe_ratio, book_value, pbv
# sql = """CREATE TABLE company_details (
#         scrip VARCHAR(100) NOT NULL,
#         name VARCHAR(100) NOT NULL,
#         sector VARCHAR(100) NOT NULL,
#         share_outstanding VARCHAR(100) NOT NULL,
#         market_price VARCHAR(100) NOT NULL,
#         percentage_change VARCHAR(100) NOT NULL,
#         last_traded_on VARCHAR(100) NOT NULL,
#         fiftytwo_week_high_low VARCHAR(100) NOT NULL,
#         onehundredeighty_day_avg VARCHAR(100) NOT NULL,
#         onehundredtwenty_day_avg VARCHAR(100) NOT NULL,
#         one_year_yield VARCHAR(100) NOT NULL,
#         eps VARCHAR(100) NOT NULL,
#         pe_ratio VARCHAR(100) NOT NULL,
#         book_value VARCHAR(100) NOT NULL,
#         pbv VARCHAR(100) NOT NULL
#         )"""

def historic_data_table_creator():
    sectors = ['corporate_debentures', 'microfinance', 'commercial_banks',
           'non_life_insurance', 'hydro_powers', 'life_insurance', 'finance',
           'tradings', 'manufacturing_and_processing', 'investment', 'hotels',
           'development_banks', 'mutual_fund', 'other']


    db = mysql.connector.connect(host='localhost', user='root', password='', database="stock")
    cursor = db.cursor()
    # database_name = 'stock'
    # cursor.execute("CREATE DATABASE "+database_name) #remove this code if you have already created a Db named 'stocks' 
    for sector in sectors:
        cursor.execute("CREATE TABLE IF NOT EXISTS "+sector +" (`Scrip` VARCHAR(10),`Time` DOUBLE, `Close` VARCHAR(20), `Open` VARCHAR(20), `High` VARCHAR(20), `Low` VARCHAR(20), `Volume` VARCHAR(20))")
        cursor.execute("DELETE FROM " + sector)
    db.commit()
    cursor.close()
    db.close()

def other_data_table_creator():
    db = mysql.connector.connect(host='localhost',user='root',password='',database="stock")
    cursor = db.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS top_gainers (`id` INT NOT NULL PRIMARY KEY,`symbol` VARCHAR(30), `company` VARCHAR(200), `ltp` VARCHAR(10), `point_change` VARCHAR(10), `percent_change` VARCHAR(10))")
    cursor.execute("CREATE TABLE IF NOT EXISTS top_losers (`id` INT NOT NULL PRIMARY KEY,`symbol` VARCHAR(30), `company` VARCHAR(200), `ltp` VARCHAR(10), `point_change` VARCHAR(10), `percent_change` VARCHAR(10))")
    cursor.execute("CREATE TABLE IF NOT EXISTS sub_indices (`sector` VARCHAR(200),`turnover` VARCHAR(50), `close` VARCHAR(50), `point` VARCHAR(10), percent_change VARCHAR(10))")
    cursor.execute("DELETE FROM `top_gainers`")
    cursor.execute("DELETE FROM `top_losers`")
    cursor.execute("DELETE FROM `sub_indices`")
    db.commit()
    cursor.close()
    db.close()

def insert_company_detail():
    db = mysql.connector.connect(
            host='localhost', user='root', password='', database="stock")
    cursor = db.cursor()
    query = "DELETE FROM company_details"
    cursor.execute(query)
    names = ['ACLBSL', 'ALBSL', 'CBBL', 'CLBSL', 'DDBL', 'FMDBL', 'FOWAD', 'GMFBS', 'GILB', 'GBLBS', 'GLBSL', 'ILBS',
             'JALPA', 'JSLBB', 'JBLB', 'KMCDB', 'KLBSL', 'LLBS', 'MLBSL', 'MSLB', 'MKLB', 'MLBS', 'MERO', 'MMFDB',
             'MLBBL', 'NSLB', 'NLBBL', 'NESDO', 'NICLBSL', 'NUBL', 'RULB', 'RMDC', 'RSDC', 'SABSL', 'SDLBSL', 'SMATA',
             'SLBSL', 'SKBBL', 'SMFDB', 'SMB', 'SWBBL', 'SMFBS', 'SLBBL', 'USLB', 'VLBS', 'WNLB', 'ADBL', 'BOKL',
             'CCBL', 'CZBIL', 'CBL', 'EBL', 'GBIME', 'KBL', 'LBL', 'MBL', 'MEGA', 'NABIL', 'NBL', 'NCCB', 'SBI',
             'NICA', 'NMB', 'PRVU', 'PCBL', 'SANIMA', 'SBL', 'SCB', 'SRBL', 'AIL', 'EIC', 'GIC', 'HGI', 'IGI', 'LGIL',
             'NIL', 'NICL', 'NLG', 'PRIN', 'PIC', 'PICL', 'RBCL', 'SIC', 'SGI', 'SICL', 'SIL', 'UIC', 'AKJCL', 'API',
             'AKPL', 'AHPC', 'BARUN', 'BNHC', 'BPCL', 'CHL', 'CHCL', 'DHPL', 'GHL', 'GLH', 'HDHPC', 'HURJA', 'HPPL',
             'JOSHI', 'KPCL', 'KKHC', 'LEC', 'MBJC', 'MKJC', 'MEN', 'MHNL', 'NHPC', 'NHDL', 'NGPL', 'NYADI', 'PMHPL',
             'PPCL', 'RADHI', 'RHPL', 'RURU', 'SAHAS', 'SPC', 'SHPC', 'SJCL', 'SSHL', 'SHEL', 'SPDL', 'TPC', 'UNHPL',
             'UMRH', 'UMHL', 'UPCL', 'UPPER', 'ALICL', 'GLICL', 'JLI', 'LICN', 'NLICL', 'NLIC', 'PLI', 'PLIC', 'RLI',
             'SLI', 'SLICL', 'ULI', 'BFC', 'CFCL', 'GFCL', 'GMFIL', 'GUFL', 'ICFC', 'JFL', 'MFIL', 'MPFL', 'NFS',
             'PFL', 'PROFL', 'RLFL', 'SFCL', 'SIFC', 'BBC', 'STC', 'BNT', 'HDL', 'SHIVM', 'UNL', 'CHDC', 'CIT', 'ENL',
             'HIDCL', 'NIFRA', 'NRN', 'CGH', 'OHL', 'SHL', 'TRH', 'CORBL', 'EDBL', 'GBBL', 'GRDBL', 'JBBL', 'KSBBL',
             'KRBL', 'LBBL', 'MLBL', 'MDB', 'MNBBL', 'NABBC', 'SAPDBL', 'SADBL', 'SHINE', 'SINDU', 'KEF', 'LUK', 'NEF',
             'NIBLPF', 'NTC', 'NRIC']
    for name in names:
        url = ("https://merolagani.com/CompanyDetail.aspx?symbol=") + name
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')
        data = soup.find(
            'table', class_='table table-striped table-hover table-zeromargin')
        detail = []
        company_name = [soup.find(
            'span', id='ctl00_ContentPlaceHolder1_CompanyDetail1_companyName').text]
        for d in data.find_all('tr'):
            if d.td is not None:
                detail.append(d.td.text.strip())
        new_list = [d.replace(' ', '').replace(
            '\r', '').replace('\n', '') for d in detail]
        new_list = [n for n in new_list if n != '']
        final_detail = [name]+company_name+new_list[0:13]
        company_ = tuple(final_detail)
        
        query = "INSERT INTO company_details VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(query, company_)
        db.commit()

    print("data inserted successfully")

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

def historic_data_populator():
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/46.0.2490.80'
   }
    sectors = {
            'corporate_debentures': ['NICAD8283'],
            'microfinance':['ACLBSL','ALBSL','CBBL','CLBSL','DDBL','FMDBL','FOWAD','GMFBS','GILB','GBLBS','GLBSL','ILBS','JALPA','JSLBB','JBLB','KMCDB','KLBSL','LLBS','MLBSL','MSLB','MKLB','MLBS','MERO','MMFDB','MLBBL','NSLB','NLBBL','NESDO','NICLBSL','NUBL','RULB','RMDC','RSDC','SABSL','SDLBSL','SMATA','SLBSL','SKBBL','SMFDB','SMB','SWBBL','SMFBS','SLBBL','USLB','VLBS','WNLB'],
            'commercial_banks':['ADBL','BOKL','CCBL','CZBIL','CBL','EBL','GBIME','KBL','LBL','MBL','MEGA','NABIL','NBL','NCCB','SBI','NICA','NMB','PRVU','PCBL','SANIMA','SBL','SCB','SRBL'],
            'non_life_insurance':['AIL','EIC','GIC','HGI','IGI','LGIL','NIL','NICL','NLG','PRIN','PIC','PICL','RBCL','SIC','SGI','SICL','SIL','UIC'],
            'hydro_powers':['AKJCL','API','AKPL','AHPC','BARUN','BNHC','BPCL','CHL','CHCL','DHPL','GHL','GLH','HDHPC','HURJA','HPPL','JOSHI','KPCL','KKHC','LEC','MBJC','MKJC','MEN','MHNL','NHPC','NHDL','NGPL','NYADI','PMHPL','PPCL','RADHI','RHPL','RURU','SAHAS','SPC','SHPC','SJCL','SSHL','SHEL','SPDL','TPC','UNHPL','UMRH','UMHL','UPCL','UPPER'],
            'life_insurance':['ALICL','GLICL','JLI','LICN','NLICL','NLIC','PLI','PLIC','RLI','SLI','SLICL','ULI'],
            'finance':['BFC','CFCL','GFCL','GMFIL','GUFL','ICFC','JFL','MFIL','MPFL','NFS','PFL','PROFL','RLFL','SFCL','SIFC'],
            'tradings':['BBC', 'STC'],
            'manufacturing_and_processing':['BNT', 'HDL', 'SHIVM', 'UNL'],
            'investment':['CHDC', 'CIT', 'ENL', 'HIDCL', 'NIFRA', 'NRN'],
            'hotels':['CGH', 'OHL', 'SHL', 'TRH'],
            'development_banks':['CORBL','EDBL','GBBL','GRDBL','JBBL','KSBBL','KRBL','LBBL','MLBL','MDB','MNBBL','NABBC','SAPDBL','SADBL','SHINE','SINDU'],
            'mutual_fund':['KEF','LUK','NEF','NIBLPF'],
            'other':['NTC', 'NRIC']
           }

    for item in sectors.items():
        sector_name = item[0]
        company_list = item[1]
        for name in company_list:
            scrip = name
            data_list = []
            start_time = 1325376000  # 2012 january 1
            end_time = 1662336000
            # https://nepsealpha.com/trading/1/history?symbol=ADBL&resolution=1D&from=1629072000&to=1661299200&pass=ok&force=23745&currencyCode=NRS
            response = requests.get(
                'https://nepsealpha.com/trading/1/history?symbol=' + scrip + '&resolution=1D&from=' + str(
                    start_time) + '&to=' + str(end_time) + '&pass=ok&force=23745&currencyCode=NRS',headers=headers)
            a = response.json()
            Open = a['o']
            High = a['h']
            Close = a['c']
            Low = a['l']
            Volume = a['v']
            Time = a['t']

            database_name = 'stock' ########## change according to need ########

            db = mysql.connector.connect(host='localhost', user='root', password='', database=database_name)
            cursor = db.cursor()
            for o, h, c, l, v, t in zip(Open, High, Close, Low, Volume, Time):
                data = (scrip, t, c, o, h, l, v)
                data_list.append(data)
            sql_query = 'INSERT INTO  '+sector_name+f' (Scrip, Time, Close, Open, High, Low, Volume) VALUES (%s, %s, %s, %s, %s, %s, %s)'

            cursor.executemany(sql_query,data_list)
            db.commit()
    cursor.close()
    db.close()





if __name__ == "__main__":
    historic_data_table_creator()
    other_data_table_creator()
    insert_company_detail()
    sub_indices()
    top_gainers()
    top_losers()
    historic_data_populator()
