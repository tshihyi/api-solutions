from datetime import date, datetime
from fastapi import FastAPI, Query
from typing import Union
import uvicorn
import sqlite3

app = FastAPI()

@app.get("/")
def read_root():
    connection = connection = sqlite3.connect('main.db')
    cursor = connection.cursor()

    #CUST_POINT_TXN
    cursor.execute("""
         SELECT * FROM CUST_POINT_TXN;
          """)
    data = cursor.fetchall()
    connection.close()
    return data

parameter_validate = {
    'account_customer_no':
    Query(default=None,
          description="身份證字號-生日, 護照號碼-生日",
          min_length=10,
          max_length=30),
    'as_of_date_start':
    Query(default=None, description="資料開始日, YYYY-MM-DD"),
    'as_of_date_end':
    Query(default=None, description="資料結束日, YYYY-MM-DD"),
    'bank_no':
    Query(default=None, description="分行代碼", max_length=5),
    'transaction_datetime_start':
    Query(default=None, description="點數交易開始日, YYYY-MM-DD HH:MM:SS"),
    'transaction_datetime_end':
    Query(default=None, description="點數交易結束日, YYYY-MM-DD HH:MM:SS"),
    'project_group_id':
    Query(default=None, description="活動代碼", max_length=80),
    'transaction_memo':
    Query(default=None, description="交易註記", max_length=80),
    'transaction_no':
    Query(default=None, description="交易編號", max_length=10),
    'source_channel':
    Query(
        default=None,
        description="H:行動APP, E:網銀, L: Line, S: 系統, O: 其他",
        enum=["H", "E", "L", "S", "O"],
    ),
    'transaction_type':
    Query(default=None, description="交易類型，HEG:兌換商品, LOTTERY:抽獎", max_length=3)
}


@app.get("/items/")
async def read_item(
    account_customer_no: Union[
        str, None] = parameter_validate["account_customer_no"],
    as_of_date_start: Union[date, None] = parameter_validate["as_of_date_start"],
    as_of_date_end: Union[date, None] = parameter_validate["as_of_date_end"],
    bank_no: Union[str, None] = parameter_validate["bank_no"],
    transaction_datetime_start: Union[
        datetime, None] = parameter_validate["transaction_datetime_start"],
    transaction_datetime_end: Union[
        datetime, None] = parameter_validate["transaction_datetime_end"],
    project_group_id: Union[str,
                            None] = parameter_validate["project_group_id"],
    transaction_memo: Union[str,
                            None] = parameter_validate["transaction_memo"],
    transaction_no: Union[str, None] = parameter_validate["transaction_no"],
    source_channel: Union[str, None] = parameter_validate["source_channel"],
    transaction_type: Union[str,
                            None] = parameter_validate["transaction_type"]):

    query_list = ['SELECT * FROM CUST_POINT_TXN WHERE 1 = 1 ']

    item = {}

    #query parameters
    if account_customer_no:
        item.update({"account_customer_no": account_customer_no + "%"})
        query_list.append(
            'AND `account_customer_no` like :account_customer_no ')
    if as_of_date_start:
        item.update({"as_of_date_start": as_of_date_start})
        query_list.append('AND `as_of_date` BETWEEN CAST(DATE(:as_of_date_start) as TEXT)')
    if as_of_date_end:
        item.update({"as_of_date_end": as_of_date_end})
        query_list.append('AND CAST(DATE(:as_of_date_end) as TEXT) ')
    if bank_no:
        item.update({"bank_no": bank_no})
        query_list.append('AND `bank_no` = :bank_no ')
    if transaction_datetime_start:
        item.update({"transaction_datetime_start": transaction_datetime_start})
        query_list.append('AND `Modify_DateTime` BETWEEN CAST(DATETIME(:transaction_datetime_start) as TEXT) ')
    if transaction_datetime_end:
        item.update({"transaction_datetime_end": transaction_datetime_end })
        query_list.append('AND CAST(DATETIME(:transaction_datetime_end) as TEXT) ')
    if project_group_id:
        item.update({"project_group_id": project_group_id + "%"})
        query_list.append('AND `project_group_id` like :project_group_id ')
    if transaction_memo:
        item.update({"transaction_memo": transaction_memo + "%"})
        query_list.append('AND `txn_memo` like :transaction_memo ')
    if transaction_no:
        item.update({"transaction_no": transaction_no + "%"})
        query_list.append('AND `txn_nbr` like :transaction_no ')
    if source_channel:
        item.update({"source_channel": source_channel + "%"})
        query_list.append(
            'AND `txn_source_channel_code` like :source_channel ')
    if transaction_type:
        item.update({"transaction_type": transaction_type + "%"})
        query_list.append('AND `txn_type_code` like :transaction_type ')

    sql_str = ' '.join(query_list)

    connection = sqlite3.connect('main.db')
    cursor = connection.cursor()
    cursor.execute(sql_str + " limit 10;", item)
    data = cursor.fetchall()
    connection.close()

    #return query result
    return list(
        map(
            lambda item: {
                "account_customer_no": item[0],
                "as_of_date": item[1],
                "atm_id": item[2],
                "bank_no": item[3],
                "mission_point": item[4],
                "modify_date": item[5],
                "modify_datetime": item[6],
                "project_group_id": item[7],
                "source_table_code": item[8],
                "total_point": item[9],
                "total_txn_cnt": item[10],
                "txn_cnt": item[11],
                "txn_memo": item[12],
                "txn_nbr": item[13],
                "txn_point": item[14],
                "txn_source_channel_code": item[15],
                "txn_type_code": item[16]
            }, data))

uvicorn.run(app, host="0.0.0.0", port=8080)
