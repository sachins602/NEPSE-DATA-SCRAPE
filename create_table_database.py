import mysql.connector

sectors = ['corporate_debentures', 'microfinance', 'commercial_banks',
           'non_life_insurance', 'hydro_powers', 'life_insurance', 'finance',
           'tradings', 'manufacturing_and_processing', 'investment', 'hotels',
           'development_banks', 'mutual_fund', 'other']


db = mysql.connector.connect(host='localhost', user='root', password='')
cursor = db.cursor()
database_name = 'stock'
# cursor.execute("CREATE DATABASE "+database_name) #remove this code if you have already created a Db named 'stocks'
for sector in sectors:
    cursor.execute("CREATE TABLE IF NOT EXISTS "+sector +" (`Scrip` VARCHAR(10),`Time` DOUBLE, `Close` VARCHAR(20), `Open` VARCHAR(20), `High` VARCHAR(20), `Low` VARCHAR(20), `Volume` VARCHAR(20))")
db.commit()
cursor.close()
db.close()
