#!python3

import pandas as pd
import numpy as np

import psycopg2
from sqlalchemy import create_engine

from mrjob.job import MRJob
from mrjob.step import MRStep

cols = 'id_transaction,id_customer,date_transaction,product_transaction,amount_transaction'.split(',')

def read_db():
    #connection
    url = 'postgresql+psycopg2://degaraja:123456789@localhost:5432/postgres'
    engine = create_engine(url)

    #read data
    df = pd.read_sql_table('bigdata_transaction', con=engine)
    data = [tuple(x) for x in df.values]
    for raw in data:
        return raw

class OrderMonthCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sort)
        ]

    def mapper(self, _, line):
        # Convert each line into a dictionary
        row = dict(zip(cols, read_db(line)))

        # Yield the date_transaction "2013-02-23"
        yield row['date_transaction'][5:7], 1

    def reducer(self, key, values):
        #for 'order_date' compute
        yield None, (key,sum(values))
    
    def sort(self, key, values):
        data = []
        for order_month, order_count in values:
            data.append((order_month, order_count))
            data.sort()

        for order_month, order_count in data:
           yield order_month, order_count

if __name__ == '__main__':
    OrderMonthCount.run()