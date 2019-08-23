import pandas_datareader as pdr
import yfinance
import pandas
import datetime
import pymysql
import numpy as np
from pandas import DataFrame
yfinance.pdr_override()
pandas.core.common.is_list_like = pandas.api.types.is_list_like


class Stock:
    def __init__(self, host, user, password, db_name):
        self.db_name = db_name
        self.good_list = ["FEYE", "NMRK", "KLXE", "TSLA", "FNKO", "SCOR", "NVCR", "KO", "WMT", "VZ", "MMM", "GS",
                          "JPM", "DIS", "IBM", "AAPL", "XOM", "INTC", "NKE", "BA"]
        self.con = pymysql.connect(host=host,
                                  user=user,
                                  password=password,
                                  db=db_name,
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)
        self.cursor = self.con.cursor()

    def get_stock_symbol(self, directory):
        # overal variables
        # print(pandas_datareader.data.get_data_yahoo('111, Inc.', start, end))
        # connecting to MySQL DB
        # SQL cursor
        # SQL queries
        # DB : stock / TABLE : stock_id / COLLUMS : symbol, name, sector, industry
        use_sql = "USE stock;"
        table_sql = "CREATE TABLE stock_id (" \
                    "stock_symbol VARCHAR(20));"
        self.run_sql(table_sql)

        # openning the downloaded file that was intstalled from the internet
        file = open(directory, 'r')
        lines = file.readlines()
        # file.split(",")               # 8 strings are the information of one type of code
        for i in range(0, 3436):  # total 3436 lines of data
            individual_line_list = lines[i]  # lines that are separated by ','
            separate_line = individual_line_list.split(",")
            # sometimes stock_name has , in its name so it causes to pu a different value
            # in its sector and industry value
            stock_symbol = separate_line[0]
            # insert into mysql
            insert_sql = "INSERT INTO stock_id (stock_symbol) VALUES (%s)"
            values = stock_symbol
            self.cursor.execute(insert_sql, values)
            self.con.commit()
        file.close()

    def db_input(self, start):
        fetch_sql = "SELECT * FROM stock_id;"
        self.cursor.execute(fetch_sql)
        self.id_data = self.cursor.fetchall()
        for i in range(1, len(self.id_data)):
            symbol = self.id_data[i]['stock_symbol']

            symbol = symbol.replace('"', "")
            if symbol in self.good_list:
                print(symbol)
                try:
                    self.get_stock_data(start, symbol)
                except pymysql.err.InternalError:
                    pass

    def run_sql(self, sql):
        try:
            self.cursor.execute(sql)
        except:
            pass

    def get_stock_data(self, start, symbol):
        # start = datetime.datetime(2010, 1 ,1)
        # creates the table
        table_sql = 'CREATE TABLE ' + symbol + '(s_date VARCHAR(20), s_open FLOAT,' \
                                               's_high FLOAT, s_low FLOAT, s_close FLOAT, ' \
                                               's_adjp FLOAT, s_volume INT);'

        self.run_sql(table_sql)

        print("Fetching . . . " + symbol)

        df = pdr.data.get_data_yahoo(symbol, start)
        # df.columns = ['s_open', 's_high', 's_low', 's_close', 's_adjp', 's_volume']
        df_CSV = DataFrame.to_csv(df)
        data_list = df_CSV.split(',')
        # this will print the first of the Open collumn.
        insert_sql = "INSERT INTO " + symbol + "(s_date, s_open, s_high, s_low, s_close, s_adjp, s_volume) " \
                                               "VALUES (%s,%s,%s,%s,%s,%s,%s);"
        for i in range(len(df)):
            date = df.index[i]

            date = datetime.datetime.strftime(date, "%Y-%m-%d")
            print(date)
            openPrice = df["Open"][i]
            highPrice = df["High"][i]
            lowPrice = df["Low"][i]
            closePrice = df["Close"][i]
            adjPrice = df["Adj Close"][i]
            volume = df["Volume"][i]

            self.cursor.execute(insert_sql, (date,
                                             np.float(openPrice),
                                             np.float(highPrice),
                                             np.float(lowPrice),
                                             np.float(closePrice),
                                             np.float(adjPrice),
                                             np.float(volume)))
            self.con.commit()

