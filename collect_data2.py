import logging
import os
import sqlite3
import threading
import warnings
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta

import dva
import res
import tsp

# 忽略警告
warnings.filterwarnings('ignore')

# 设置日志记录器
logging.basicConfig(filename='database.log', level=logging.INFO)


def db_operation(func):
    """decorator
    :param func:
    :return:
    """

    def wrapper(*args, **kwargs):
        # 连接数据库
        conn = sqlite3.connect('mes1.db')
        try:
            # 执行数据库操作
            result = func(conn, *args, **kwargs)
            # 提交事务并关闭连接
            conn.commit()
        except Exception as e:
            # 回滚事务并关闭连接
            conn.rollback()
            raise e
        finally:
            # 关闭连接
            conn.close()
            # 记录日志
            logging.info(f"{func.__name__} executed successfully")
        return result

    return wrapper


@db_operation
def create_table(conn):
    """ create table
    :param conn:
    :return:
    """
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS record (
                   fixture_id TEXT, 
                   stop_time TIMESTAMP, 
                   result TEXT,
                   sn TEXT,
                   sw_version TEXT,
                   failure_message TEXT,
                   Carrier_sn TEXT,
                   test_station TEXT)""")
    conn.commit()


@db_operation
def insert_records(conn, series):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO record VALUES (?, ? ,? ,? ,? ,?, ?, ?)", tuple(series))
    conn.commit()


@db_operation
def search_by_sn(conn, sn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM record WHERE sn = '{sn}'")
    result = cursor.fetchall()
    return result


@db_operation
def get_records(conn, start, end, sql):
    cursor = conn.cursor()
    cursor.execute(sql, (start, end))
    result = cursor.fetchall()
    return result


def is_table_exists(db_name, table_name):
    if not os.path.exists(db_name):
        print(f"{db_name} does not exist")
        return False
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    result = cursor.fetchone()
    conn.close()
    if result:
        return True
    else:
        return False


def check_row_exists(series, db_name, table_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    row_values = series.tolist()
    row_index = series.index
    query = f"SELECT * FROM {table_name} WHERE " + " AND ".join([f"{col} = ?" for col in row_index])
    cursor.execute(query, row_values)
    result = cursor.fetchone()
    conn.close()
    if result:
        return True
    else:
        return False


def check_sn_exists(sn, db_name, table_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = f"SELECT sn FROM {table_name} WHERE sn='{sn}'"
    cursor.execute(query)
    result = cursor.fetchone()
    conn.close()
    if result:
        return True
    else:
        return False


def process_file(folder_path, filename):

    if filename.endswith(".xlsx"):
        file_path = os.path.join(folder_path, filename)
        try:
            # Open the xlsx file
            data = pd.read_excel(file_path)
            # Get the necessary information from the "数据信息" column
            info_list = data['数据信息'].tolist()
            df = pd.DataFrame()
            # Convert the json list to dataframe
            if 'res' in filename or 'RES' in filename:
                json_to_dataframe = res.JsonToListDataFrame(info_list)
                df = json_to_dataframe.generate_dataframe()
            elif 'dva' in filename or 'DVA' in filename:
                json_to_dataframe = dva.JsonToListDataFrame(info_list)
                df = json_to_dataframe.generate_dataframe()
            elif 'tsp' in filename or 'TSP' in filename:
                json_to_dataframe = tsp.JsonToListDataFrame(info_list)
                df = json_to_dataframe.generate_dataframe()
                df = df.reindex(
                    columns=['fixture_id', 'stop_time', 'result', 'sn', 'sw_version', 'failure_message', 'Carrier_sn',
                             'test_station'])
            # df['stop_time'] = pd.to_datetime(df['stop_time'])
            if not is_table_exists('mes1.db', 'record'):
                create_table()
            for i in tqdm(range(len(df)), mininterval=2):
                row = df.iloc[i]
                if not check_row_exists(row, 'mes1.db', 'record'):
                    insert_records(row)
        except Exception as e:
            print(e)
        finally:
            os.remove(file_path)


def add_from_xlsx(folder_path):
    threads = []
    # Loop through each file in the folder
    for filename in os.listdir(folder_path):
        t = threading.Thread(target=process_file, args=(folder_path, filename))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("所有文件处理完成")


#
folder = "xlsx_files"
add_from_xlsx(folder)


# print(search_by_sn('G9P3503G0QL21KHA6'))

#  create_table()
# start_time = '2024-01-07 00:00:00'
# end_time = '2024-01-07 00:19:09'
# station_name = 'TSP-E'
# # sql_query = "SELECT * FROM record WHERE stop_time BETWEEN? AND? AND test_station =? AND result = 'FAIL'"
# sql_query = "SELECT DISTINCT sn FROM record AND test_station ='TSP-E' "
#

@db_operation
def create_table1(conn):
    """ create table
    :param conn:
    :return:
    """
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS record1 (
                   sn TEXT,
                   stop_time TIMESTAMP, 
                   result_type TEXT)""")
    conn.commit()


@db_operation
def insert_record1(conn, lst):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO record1 VALUES (?, ? ,? )", tuple(lst))
    conn.commit()


# create_table1()


def generate_record1(n):
    # 连接数据库
    conn = sqlite3.connect('mes1.db')
    station = 'TSP-E'
    sql_query = f"SELECT DISTINCT sn FROM record WHERE test_station ='{station}' "
    # 查询数据

    df = pd.read_sql_query(sql_query, conn)
    sn_list = df['sn'].tolist()

    for item in tqdm(sn_list[n:-1], desc="analysis records", colour='green', mininterval=1):
        if not check_sn_exists(item, 'mes1.db', 'record1'):
            sql = f"SELECT * FROM record WHERE sn='{item}' AND test_station ='TSP-E' ORDER BY stop_time DESC"
            sorted_df = pd.read_sql_query(sql, conn)
            recent_result = sorted_df.iloc[0]['result']
            if len(sorted_df) == 1:
                if recent_result == 'PASS':
                    insert_record1([item, sorted_df.iloc[0]['stop_time'], 'PASS'])
                else:
                    insert_record1([item, sorted_df.iloc[0]['stop_time'], 'TO_BE_TESTING'])
            elif 1 < len(sorted_df) < 4:
                if recent_result == 'PASS':
                    insert_record1([item, sorted_df.iloc[0]['stop_time'], 'RETEST'])
                else:
                    insert_record1([item, sorted_df.iloc[0]['stop_time'], 'TO_BE_TESTING'])
            elif len(sorted_df) == 4:
                if recent_result == 'PASS':
                    insert_record1([item, sorted_df.iloc[0]['stop_time'], 'PASS'])
                else:
                    insert_record1([item, sorted_df.iloc[0]['stop_time'], 'FAIL'])
            elif len(sorted_df) > 4:
                insert_record1([item, sorted_df.iloc[0]['stop_time'], 'FAIL'])
    conn.close()

# generate_record1(145956)

def get_fpy(start_time, end_time):
    conn = sqlite3.connect('mes1.db')
    sql_query = f"SELECT COUNT(*) FROM record1 WHERE stop_time BETWEEN? AND? "
    result = get_records(start_time, end_time, sql_query)
    input_count = result[0][0]
    if input_count == 0:
        return 0, 0, 0, 0, 0, 0, 0, 0
    # print(f'input:\t{input_count}')

    sql_query = f"SELECT COUNT(*) FROM record1 WHERE stop_time BETWEEN? AND? AND result_type = 'PASS' "
    result = get_records(start_time, end_time, sql_query)
    pass_count = result[0][0]
    pass_rate = pass_count / input_count
    # print(f'pass:\t{pass_count}\t\tYeild:\t{pass_rate:.2%}')

    sql_query = f"SELECT COUNT(*) FROM record1 WHERE stop_time BETWEEN? AND? AND result_type = 'FAIL'"
    result = get_records(start_time, end_time, sql_query)
    fail_count = result[0][0]
    fail_rate = fail_count / input_count
    # print(f'fail:\t{fail_count}\t\tfail_rate:\t{fail_rate:.2%}')

    sql_query = f"SELECT COUNT(*) FROM record1 WHERE stop_time BETWEEN? AND? AND result_type = 'RETEST'"
    result = get_records(start_time, end_time, sql_query)
    retest_count = result[0][0]
    retest_rate = retest_count / input_count
    # print(f'retest:\t{retest_count}\t\tretest_rate:\t{retest_rate:.2%}')

    sql_query = f"SELECT COUNT(*) FROM record1 WHERE stop_time BETWEEN? AND? AND result_type = 'TO_BE_TESTING'"
    result = get_records(start_time, end_time, sql_query)
    testing_count = result[0][0]

    # print(f'tesing:\t{testing_count}')

    conn.close()
    return input_count, pass_count, pass_rate, fail_count, fail_rate, retest_count, retest_rate, testing_count


def get_fpy_time_period(start, end):
    start_datetime = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_datetime = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    delta = timedelta(days=1)
    current_datetime = start_datetime
    time_periods = []

    while current_datetime <= end_datetime:
        current_time_start = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
        current_datetime_end = current_datetime + delta
        current_time_end = current_datetime_end.strftime('%Y-%m-%d %H:%M:%S')
        input_count, pass_count, pass_rate, fail_count, fail_rate, retest_count, retest_rate, testing_count = get_fpy(
            current_time_start, current_time_end)
        time_periods.append((current_time_start, current_time_end, input_count, pass_count, pass_rate, fail_count,
                             fail_rate, retest_count, retest_rate, testing_count))
        current_datetime += delta

    return time_periods


# start = '2024-01-14 00:00:00'
# end = '2024-01-14 23:59:59'
# get_fpy(start, end)
# print("current_time_start,current_time_end,input_count,pass_count,pass_rate,fail_count,fail_rate,retest_count,retest_rate,testing_count")
# result = get_fpy_time_period(start, end)
# for i in result:
#     print(i)

# add_from_xlsx('xlsx_files')
#  generate_record1(37200)

def search_top_carrier(start, end, station):
    # Connect to the SQLite database
    conn = sqlite3.connect('mes1.db')

    # Create a cursor object
    cur = conn.cursor()
    # df = pd.DataFrame()

    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        # Execute a SELECT query to retrieve data where stop_time is between the time range
        cur.execute("SELECT * FROM record WHERE stop_time BETWEEN ? AND ? AND test_station = ? AND result='FAIL'",
                    (start_time, end_time, station))
        # Fetch all the rows that match the condition
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        top_failcount = df['Carrier_sn'].value_counts()

    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_failcount


def search_top_tester(start, end, station):
    # Connect to the SQLite database
    conn = sqlite3.connect('mes1.db')

    # Create a cursor object
    cur = conn.cursor()
    df = pd.DataFrame()

    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        # Execute a SELECT query to retrieve data where stop_time is between the time range
        cur.execute("SELECT * FROM record WHERE stop_time BETWEEN ? AND ? AND test_station = ? AND result='FAIL'",
                    (start_time, end_time, station))
        # Fetch all the rows that match the condition
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        top_failcount = df['fixture_id'].value_counts()

    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_failcount


def search_top_failure(start, end, station):
    # Connect to the SQLite database
    conn = sqlite3.connect('mes1.db')

    # Create a cursor object
    cur = conn.cursor()
    df = pd.DataFrame()

    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        # Execute a SELECT query to retrieve data where stop_time is between the time range
        cur.execute("SELECT * FROM record WHERE stop_time BETWEEN ? AND ? AND test_station = ? AND result='FAIL'",
                    (start_time, end_time, station))
        # Fetch all the rows that match the condition
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        top_failcount = df['failure_message'].value_counts()

    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_failcount


def failure_count_of_tester(start, end, tester, station):
    conn = sqlite3.connect('mes1.db')
    cur = conn.cursor()
    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        cur.execute("SELECT * FROM record WHERE stop_time BETWEEN ? AND ? AND result='FAIL' AND fixture_id = ? AND "
                    "test_station = ?",
                    (start_time, end_time, tester, station))
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        top_failcount = df['failure_message'].value_counts()
    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_failcount


def carrier_count_of_tester(start, end, tester, station):
    conn = sqlite3.connect('mes1.db')
    cur = conn.cursor()
    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        cur.execute("SELECT * FROM record WHERE stop_time BETWEEN ? AND ? AND result='FAIL' AND fixture_id = ? AND "
                    "test_station = ?",
                    (start_time, end_time, tester, station))
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        top_failcount = df['Carrier_sn'].value_counts()
    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_failcount


def tester_count_of_failure(start, end, failure, station):
    conn = sqlite3.connect('mes1.db')
    cur = conn.cursor()
    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        cur.execute("SELECT * FROM record WHERE stop_time BETWEEN ? AND ? AND result='FAIL' AND failure_message = ? AND "
                    "test_station = ?",
                    (start_time, end_time, failure, station))
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        top_failcount = df['fixture_id'].value_counts()
    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_failcount


def carrier_count_of_failure(start, end, failure, station):
    conn = sqlite3.connect('mes1.db')
    cur = conn.cursor()
    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        cur.execute("SELECT * FROM record WHERE stop_time BETWEEN ? AND ? AND result='FAIL' AND failure_message = ? AND "
                    "test_station = ?",
                    (start_time, end_time, failure, station))
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        top_failcount = df['Carrier_sn'].value_counts()
    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_failcount

# start = '2023-12-25 00:00:00'
# end = '2024-01-09 23:59:59'
# station = 'TSP-E'
# tester = '100301'
# # failure_message = 'FSTestProbeFsItems_OOS'
# failure_message='OpenShortTestOOS'
# fail_count = search_top_tester(start,end,station)
# fail_count = carrier_count_of_tester(start, end, tester, station)
# fail_count = failure_count_of_tester(start, end, tester, station)
# fail_count = search_top_failure(start, end, station)
# fail_count = carrier_count_of_failure(start, end, failure_message, station)
# fail_count = tester_count_of_failure(start, end, failure_message, station)
# print("tester:",tester,fail_count)


# generate_record1(90730)
#
# start = '2024-01-02 20:00:00'
# end = '2024-01-09 20:00:00'
# station = 'TSP-E'
# testers = ['100101', '100102', '100103', '100104', '100105', '100106', '100201', '100202', '100203', '100204',
#            '100205', '100206', '100301', '100302', '100303', '100304', '100305', '100306', '100401', '100402', '100403',
#            '100404', '100405', '100406', '100501', '100502', '100503', '100504', '100505', '100506', '100601', '100602']
# # failure_message = 'FSTestProbeFsItems_OOS'
# failure_message='OpenShortTestOOS'
# for tester in testers:
#     fail_count = failure_count_of_tester(start, end, tester, station)
#     print(tester, fail_count)
# fail_count = failure_count_of_tester(start, end, '100302', station)
# print(fail_count)
# FSTestProbeFsItems_OOS           63  -------->Fixture--->
# DisplayPowerOnFailed             21
# DisplayPowerOnFailedGpioCheck    18
# OpenShortTestOOS                 14
# FSCalErrorItems_OOS               8
# FailToReceiveDriftAlarm           6
# FailedToFindMtDevice              5
# CsigDataCaptureFailed             4
# FSTestRxCmItems_OOS               3
# FSTestCalTestProbeItems_OOS       3
# PowerTestOOS                      3
# PreCritialErrFailed               2
# DTN_PCOEFF_OOS                    2
# FailedToGetReport                 2
# MalibuHidMcuResetFail             1
# PF_DetectedOnModule               1
# AID_SelfTestReturnedNoResults     1
# RegionOnzOOS                      1
# AID_FailToGetLastError            1
# TRSpecOutOfTolerance              1
# DisplaySetImageFailed             1
# PowerImpedanceOOS                 1
# EepromReadFailed                  1
#
# start = '2024-01-02 20:00:00'
# end = '2024-01-09 20:00:00'
# station = 'TSP-E'
# input_count, pass_count, pass_rate, fail_count, fail_rate, retest_count, retest_rate, testing_count=get_fpy(start, end)
# print(input_count, pass_count, pass_rate, fail_count, fail_rate, retest_count, retest_rate, testing_count)