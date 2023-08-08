import requests       
import pandas as pd   
import psycopg2
import json           
import numpy as np    
from datetime import datetime  
from pathlib import Path

BASE_URL = 'https://api.exchangerate.host/timeseries?'

start_date = '2023-01-01'
end_date = '2023-01-31'
currency = ['BTC', 'CNY', 'AED'] 
symbols = 'RUB'
format = 'CSV'

for base in currency:
    response = requests.get(BASE_URL, params={'base': base,
                                      'start_date': start_date,
                                      'end_date': end_date,
                                      'symbols': symbols,
                                      'format': format
                                })
    file = open(f'./files/jan2023_{base}.csv', 'wb')              
    file.write(response.content)
    file.close()

for base in currency:
    df = pd.read_csv(f'./files/jan2023_{base}.csv', delimiter=',', decimal=',', index_col=False)
    df = pd.DataFrame(df)
    df.to_csv(f'./files/jan2023_{base}.csv', decimal=',', index=False)
    print(df.head(2)) 

data_dir = Path('./files/')
df1 = pd.concat([pd.read_csv(f) for f in data_dir.glob("*.csv")], ignore_index=True) 
df2=df1.drop(['start_date', 'end_date'], axis=1) 
df2.to_csv('./files/jan2023_total.csv', sep=',', decimal=',', index=False)
df2['rate']= df2['rate'].str.replace(',', '.').astype(float)
                                    
  finalDict = list()
for base in currency:
    df3 = pd.read_csv(f'./files/jan2023_{base}.csv', sep=',', decimal=',', index_col=False)
    df3 = pd.DataFrame(df3) 
    day_max = df3['date'].max(axis=0)         # проведем анализ по валютам: день максимума, день минимума, 
    day_min = df3['date'].min(axis=0)
    min_rate = df3['rate'].min()                       # максимальное, минимальное
    max_rate = df3['rate'].max()
    df3['rate'] = pd.to_numeric(df3['rate'], downcast="float")
    avg_rate=round(df3['rate'].mean(),2)                        # и среднее значение
    data = {'Currency': base,'Day_max rate': day_max,'Max rate': max_rate, 'Day_min rate': day_min, 'Min rate': min_rate,'Avg rate':avg_rate} # создадим датафрейм
    finalDict.append(data)          
    df4=pd.DataFrame.from_dict(finalDict, orient='columns')
    df4.to_csv('./files/jan2023_analysis.csv', decimal=',', index=False)

db_con = psycopg2.connect(database='exrate',     # создадим подключение к созданной базе данных exrate
                        user='postgres',
                        password='pass',
                        host='localhost',
                        port=5435)
cur = db_con.cursor()

cur.execute(""" CREATE TABLE rates_jan2023(
    date DATE,
    val_id VARCHAR,
    base_rate DECIMAL,
    val_base VARCHAR
)
""") 
db_con.commit() 

cur.execute(""" COPY rates_jan2023 
FROM '/tmp/jan2023_total.csv'
DELIMITER ';'
CSV HEADER;
""")
db_con.commit()

cur.execute("""                                     
CREATE TABLE rates_analysis(
    Currency VARCHAR,
    Date_max DATE ,
    Max_rate DECIMAL,
    Date_min DATE ,
    Min_rate DECIMAL,
    Avg_rate DECIMAL
    
)
""")  
db_con.commit() 

cur.execute(""" COPY rates_analysis 
FROM '/tmp/jan2023_analysis.csv'
DELIMITER ';'
CSV HEADER;""")
db_con.commit()                              
