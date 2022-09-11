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
    'Account_Customer_No':
    Query(default=None,
          description="身份證字號-生日, 護照號碼-生日",
          min_length=10,
          max_length=30),
    'As_Of_Date_Start':
    Query(default=None, description="資料開始日, YYYY-MM-DD"),
    'As_Of_Date_End':
    Query(default=None, description="資料結束日, YYYY-MM-DD"),
    'Atm_id':
    Query(default=None, description="ATM編號", max_length=8),
    'Bank_Nbr':
    Query(default=None, description="分行代碼", max_length=5),
    'Transaction_DateTime_Start':
    Query(default=None, description="點數交易開始日, YYYY-MM-DD HH:MM:SS"),
    'Transaction_DateTime_End':
    Query(default=None, description="點數交易結束日, YYYY-MM-DD HH:MM:SS"),
    'Project_Group_Id':
    Query(default=None, description="活動代碼", max_length=80),
    'Source_Table':
    Query(
        default=2,
        description="來源資料表：點數中台資料表",
        enum=["1", "2", "3"],
    ),
    'Transaction_Memo':
    Query(default=None, description="交易註記", max_length=80),
    'Transaction_Nbr':
    Query(default=None, description="交易編號", max_length=10),
    'Source_Channel':
    Query(
        default=None,
        description="H:行動APP, E:網銀, L: Line, S: 系統, O: 其他",
        enum=["H", "E", "L", "S", "O"],
    ),
    'Transaction_Type':
    Query(default=None, description="交易類型，HEG:兌換商品, LOTTERY:抽獎", max_length=3)
}


@app.get("/items/")
async def read_item(
    Account_Customer_No: Union[
        str, None] = parameter_validate["Account_Customer_No"],
    As_Of_Date_Start: Union[date, None] = parameter_validate["As_Of_Date_Start"],
    As_Of_Date_End: Union[date, None] = parameter_validate["As_Of_Date_End"],
    Bank_Nbr: Union[str, None] = parameter_validate["Bank_Nbr"],
    Transaction_DateTime_Start: Union[
        datetime, None] = parameter_validate["Transaction_DateTime_Start"],
    Transaction_DateTime_End: Union[
        datetime, None] = parameter_validate["Transaction_DateTime_End"],
    Project_Group_Id: Union[str,
                            None] = parameter_validate["Project_Group_Id"],
    Source_Table: Union[str, None] = parameter_validate["Source_Table"],
    Transaction_Memo: Union[str,
                            None] = parameter_validate["Transaction_Memo"],
    Transaction_Nbr: Union[str, None] = parameter_validate["Transaction_Nbr"],
    Source_Channel: Union[str, None] = parameter_validate["Source_Channel"],
    Transaction_Type: Union[str,
                            None] = parameter_validate["Transaction_Type"]):

    query_list = ['SELECT * FROM CUST_POINT_TXN WHERE 1 = 1 ']

    item = {}

    #query parameters
    if Account_Customer_No:
        item.update({"Account_Customer_No": Account_Customer_No + "%"})
        query_list.append(
            'AND `Account_Customer_No` like :Account_Customer_No ')
    if As_Of_Date_Start:
        item.update({"As_Of_Date_Start": As_Of_Date_Start})
        query_list.append('AND `As_Of_Date` BETWEEN CAST(DATE(:As_Of_Date_Start) as TEXT)')
    if As_Of_Date_End:
        item.update({"As_Of_Date_End": As_Of_Date_End})
        query_list.append('AND CAST(DATE(:As_Of_Date_End) as TEXT) ')
    if Bank_Nbr:
        item.update({"Bank_Nbr": Bank_Nbr})
        query_list.append('AND `Bank_Nbr` = :Bank_Nbr ')
    if Transaction_DateTime_Start:
        item.update({"Transaction_DateTime_Start": Transaction_DateTime_Start})
        query_list.append('AND `Modify_DateTime` BETWEEN CAST(DATETIME(:Transaction_DateTime_Start) as TEXT) ')
    if Transaction_DateTime_End:
        item.update({"Transaction_DateTime_End": Transaction_DateTime_End })
        query_list.append('AND CAST(DATETIME(:Transaction_DateTime_End) as TEXT) ')
    if Project_Group_Id:
        item.update({"Project_Group_Id": Project_Group_Id + "%"})
        query_list.append('AND `Project_Group_Id` like :Project_Group_Id ')
    if Source_Table:
        item.update({"Source_Table": Source_Table})
        query_list.append('AND `Source_Table_Code` = :Source_Table ')
    if Transaction_Memo:
        item.update({"Transaction_Memo": Transaction_Memo + "%"})
        query_list.append('AND `Txn_Memo` like :Transaction_Memo ')
    if Transaction_Nbr:
        item.update({"Transaction_Nbr": Transaction_Nbr + "%"})
        query_list.append('AND `Txn_Nbr` like :Transaction_Nbr ')
    if Source_Channel:
        item.update({"Source_Channel": Source_Channel + "%"})
        query_list.append(
            'AND `Txn_Source_Channel_Code` like :Source_Channel ')
    if Transaction_Type:
        item.update({"Transaction_Type": Transaction_Type + "%"})
        query_list.append('AND `Txn_Type_Code` like :Transaction_Type ')

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
                "Account_Customer_No": item[0],
                "As_Of_Date": item[1],
                "Atm_Id": item[2],
                "Bank_Nbr": item[3],
                "Mission_Point": item[4],
                "Modify_Date": item[5],
                "Modify_DateTime": item[6],
                "Project_Group_Id": item[7],
                "Source_Table_Code": item[8],
                "Total_Point": item[9],
                "Total_Txn_Cnt": item[10],
                "Txn_Cnt": item[11],
                "Txn_Memo": item[12],
                "Txn_Nbr": item[13],
                "Txn_Point": item[14],
                "Txn_Source_Channel_Code": item[15],
                "Txn_Type_Code": item[16]
            }, data))

uvicorn.run(app, host="0.0.0.0", port=8080)
