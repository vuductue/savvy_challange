import pandas as pd 
import numpy as np
import mysql.connector
import datetime
import uuid

def import_data():
    import_df = pd.read_excel('source_tickets.xlsx', sheet_name='in')

    ## transform dataframe
    import_df["converted_dt"] = pd.to_datetime(import_df["utc_timestamp"], unit='s')
    import_df['id'] = import_df.apply(lambda x: uuid.uuid4().bytes, axis=1)
    import_df['order_id'] = import_df.apply(lambda x: uuid.uuid4().bytes, axis=1)
    output_df = import_df[['id','order_id','converted_dt', 'barcode', 'cost']]

    ## create mysql connection
    conn = mysql.connector.connect(host='savvy', user='admin', password='password', database='savvydb')
    cur = conn.cursor()
    
    for row_num in range(len(output_df)):
        input_id = output_df.iloc[row_num,:]['id']
        input_order_id = output_df.iloc[row_num,:]['order_id']
        input_created = output_df.iloc[row_num,:]['converted_dt']
        input_barcode = output_df.iloc[row_num,:]['barcode']
        input_price = output_df.iloc[row_num,:]['cost']

        sql = "INSERT INTO ticket (id, order_id, created, barcode, price) VALUES (%s, %s, %s)"
        cur.execute(sql, (input_id, input_order_id,input_created, input_barcode, input_price))
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    import_data()