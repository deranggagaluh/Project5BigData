import pandas as pd
import numpy as np
import os
import psycopg2
from sqlalchemy import create_engine

if __name__ == "__main__":
    listfiledump = ['bigdata_customer','bigdata_product','bigdata_transaction']

    for itemfile in listfiledump:
        path = os.getcwd()+'/data/'
        df = pd.read_csv(path + itemfile +'.csv')

    #Connection
        url = 'postgresql+psycopg2://degaraja:123456789@localhost:5432/postgres'
        engine = create_engine(url)

        try:
            df.to_sql(itemfile, index=False, if_exists='replace', con=engine)
            print(f"Success dump {itemfile} data to database")
        except:
            print(f"Failed dump {itemfile} data to database")