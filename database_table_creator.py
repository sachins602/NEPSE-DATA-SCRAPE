import mysql.connector

db = mysql.connector.connect(host='localhost',user='root',password='',database="stock")
cursor = db.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS top_gainers (`id` INT NOT NULL PRIMARY KEY,`symbol` VARCHAR(30), `company` VARCHAR(200), `ltp` VARCHAR(10), `point_change` VARCHAR(10), `percent_change` VARCHAR(10))")
cursor.execute("CREATE TABLE IF NOT EXISTS top_losers (`id` INT NOT NULL PRIMARY KEY,`symbol` VARCHAR(30), `company` VARCHAR(200), `ltp` VARCHAR(10), `point_change` VARCHAR(10), `percent_change` VARCHAR(10))")
cursor.execute("CREATE TABLE IF NOT EXISTS sub_indices (`sector` VARCHAR(200),`turnover` VARCHAR(50), `close` VARCHAR(50), `point` VARCHAR(10), percent_change VARCHAR(10))")
db.commit()
cursor.close()
db.close()


