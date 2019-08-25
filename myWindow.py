import pandas as pd
import datetime
import stock
import numpy as np
import matplotlib.pyplot as plt


class Graph:
    def __init__(self, start):
        self.start = start.strftime("%m/%d/%Y")
        self.nd = datetime.date.today()
        self.stockObj = stock.Stock('localhost', 'root', '1llusion47', 'stock')
        self.cursor = self.stockObj.con.cursor()

    def fetch_data(self, symbol):
        self.stock_data = []
        select_sql = "SELECT * FROM " + symbol + ";"
        self.cursor.execute(select_sql)
        self.stock_data = self.cursor.fetchall()
        print(self.stock_data)

        return self.stock_data

    def graph(self, stock_data):
        adj_price_list = []
        volume_list = []
        high_list = []
        close_list = []
        open_list = []
        low_list = []
        date_list = []
        x_list = []
        for i in range(len(self.stock_data)):
            volume_list.append(stock_data[i]['s_volume'])
            adj_price_list.append(stock_data[i]['s_adjp'])
            date_list.append(stock_data[i]['s_date'])
            high_list.append(stock_data[i]['s_high'])
            close_list.append(stock_data[i]['s_close'])
            open_list.append(stock_data[i]['s_open'])
            low_list.append(stock_data[i]['s_low'])
            x_list.append(i)


        plt.plot(x_list, adj_price_list, c="green", label="adj price")

        # plt.plot(y_list, high_list, c="red", label="high price")

        plt.xlabel("x")
        plt.ylabel("y")
        plt.legend(loc='upper left')
        plt.title("stock chart")
        plt.show()


if __name__ == "__main__":
    start = datetime.datetime(2019, 3, 20)
    figure = Graph(start)
    data = figure.fetch_data("SCOR")
    figure.graph(data)

