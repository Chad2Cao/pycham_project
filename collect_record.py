import glob
import logging
import os
import sqlite3
import warnings
from datetime import datetime, timedelta

import pandas as pd
from tqdm import tqdm

############collect record from insight csv files
#just
# 忽略警告
warnings.filterwarnings('ignore')

# 设置日志记录器
logging.basicConfig(filename='insight_record.log', level=logging.INFO)


def db_operation(func):
    """decorator
    :param func:
    :return:
    """

    def wrapper(*args, **kwargs):
        # 连接数据库
        conn = sqlite3.connect('insight.db')
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
                   'Serial Number' TEXT,
                   'Test Result' TEXT,
                   'Test End Time' TIMESTAMP,
                   'Fixture ID' TEXT,
                   'Test Software Version' TEXT,
                   'Sub-test' TEXT,
                   'Sub-sub-test' TEXT,
                   'Fail Message' TEXT,
                   'Value' REAL,
                   'Lower Limit' REAL,
                   'Upper Limit' REAL)""")
    conn.commit()


@db_operation
def insert_data(conn, data):
    data.to_sql('record', conn, if_exists='append', index=False)
    conn.commit()


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


def csv_to_database(folder_path, filename):
    if filename.endswith(".csv"):
        file_path = os.path.join(folder_path, filename)
        print(f"Processing {file_path}")
        try:
            # Open the csv file
            data = pd.read_csv(str(file_path))
            to_delete = ['Product Code', 'Product Assembly Type', 'Start Time', 'Site', 'product', 'Station ID',
                         'Station Name', 'Units', 'start_time', 'station_id', 'Station Type', 'Test Head ID', 'Test',
                         'Parent Special Build Name', 'Child Special Build Name', 'Configuration Code',
                         'Week Of Manufacture']
            for cloumn in to_delete:
                if cloumn in data.columns:
                    data = data.drop(cloumn, axis=1)
            if not is_table_exists('insight.db', 'record'):
                create_table()
                insert_data(data)
            else:
                insert_data(data)
            print(f"{filename} has been successfully inserted into database")
        except Exception as e:
            print(e)
        finally:
            os.remove(file_path)


def add_from_files(folder_path):
    # Loop through each file in the folder
    for filename in tqdm(os.listdir(folder_path), desc="add csv to database", mininterval=1):
        csv_to_database(folder_path, filename)
    # print("all files has been processed successfully")


#
path = 'csv_files/record'
add_from_files(path)
# create_table()
@db_operation
def get_records(conn, sql,start_time, end_time ):
    cursor = conn.cursor()
    cursor.execute(sql, (start_time,end_time))
    result = cursor.fetchall()
    return result

@db_operation
def get_records_by_tester(conn, sql, start_time, end_time, tester):
    cursor = conn.cursor()
    cursor.execute(sql, (start_time,end_time,tester))
    result = cursor.fetchall()
    return result

def get_fpy_by_tester(start_time, end_time):
    conn = sqlite3.connect('insight.db')

    testers = ['100101', '100102', '100103', '100104', '100105', '100106', '100201', '100202', '100203', '100204',
                          '100205', '100206', '100301', '100302', '100303', '100304', '100305', '100306', '100401', '100402', '100403',
                          '100404', '100405', '100406', '100501', '100502', '100503', '100504', '100505', '100506', '100601', '100602']
    testers_fpy = pd.DataFrame(index=testers, columns=['input_count', 'pass_count', 'pass_rate', 'fail_count', 'fail_rate', 'retest_count', 'retest_rate','fail_msg'])
    for terster in testers:
        sql_query = f"""SELECT COUNT(*) FROM record 
                            WHERE "Test End Time" BETWEEN? AND? 
                            AND "Fixture ID" = ? """
        result = get_records_by_tester(sql_query,start_time, end_time,terster)
        input_count = result[0][0]
        testers_fpy.loc[terster]['input_count'] = input_count
        if input_count == 0:
            testers_fpy.loc[terster] = 0, 0, 0, 0, 0, 0, 0,'none'
            continue

        sql_query = f"""SELECT COUNT(*) FROM record 
                        WHERE "Test End Time" BETWEEN? AND? 
                        AND "Fixture ID" = ? 
                        AND "Test Result" = 'PASS' """
        result = get_records_by_tester(sql_query,start_time, end_time,terster)
        pass_count = result[0][0]
        testers_fpy.loc[terster]['pass_count'] = pass_count
        testers_fpy.loc[terster]['pass_rate'] = pass_count / input_count

        sql_query = f"""SELECT COUNT(*) FROM record 
                                WHERE "Test End Time" BETWEEN? AND? 
                                AND "Fixture ID" = ? 
                                AND "Test Result" = 'FAIL' """
        result = get_records_by_tester(sql_query, start_time, end_time, terster)
        fail_count = result[0][0]
        testers_fpy.loc[terster]['fail_count'] = fail_count
        testers_fpy.loc[terster]['fail_rate'] = fail_count / input_count

        sql_query = f"""SELECT COUNT(*) FROM record 
                                        WHERE "Test End Time" BETWEEN? AND? 
                                        AND "Fixture ID" = ? 
                                        AND "Test Result" = 'RETEST' """
        result = get_records_by_tester(sql_query, start_time, end_time, terster)
        retest_count = result[0][0]
        testers_fpy.loc[terster]['retest_count'] = retest_count
        testers_fpy.loc[terster]['retest_rate'] = retest_count / input_count

        testers_fpy.loc[terster]['fail_msg'] =failure_count_of_tester(start_time, end_time,"RETEST", terster)

    return testers_fpy.sort_values(by="retest_rate", ascending=False)


def get_fpy(start_time, end_time):
    conn = sqlite3.connect('insight.db')
    sql_query = f'SELECT COUNT(*) FROM record WHERE "Test End Time" BETWEEN? AND? '
    result = get_records( sql_query,start_time, end_time)
    input_count = result[0][0]
    if input_count == 0:
        return 0, 0, 0, 0, 0, 0, 0
    # print(f'input:\t{input_count}')

    sql_query = f'SELECT COUNT(*) FROM record WHERE "Test End Time" BETWEEN? AND? AND "Test Result" = "PASS" '
    result = get_records(sql_query, start_time, end_time)
    pass_count = result[0][0]
    pass_rate = pass_count / input_count
    # print(f'pass:\t{pass_count}\t\tYeild:\t{pass_rate:.2%}')

    sql_query = f'SELECT COUNT(*) FROM record WHERE "Test End Time" BETWEEN? AND? AND "Test Result" = "FAIL" '
    result = get_records(sql_query, start_time, end_time)
    fail_count = result[0][0]
    fail_rate = fail_count / input_count
    # print(f'fail:\t{fail_count}\t\tfail_rate:\t{fail_rate:.2%}')

    sql_query = f'SELECT COUNT(*) FROM record WHERE "Test End Time" BETWEEN? AND? AND "Test Result" = "RETEST" '
    result = get_records(sql_query, start_time, end_time)
    retest_count = result[0][0]
    retest_rate = retest_count / input_count
    # print(f'retest:\t{retest_count}\t\tretest_rate:\t{retest_rate:.2%}')

    # print(f'tesing:\t{testing_count}')

    conn.close()
    return input_count, pass_count, pass_rate, fail_count, fail_rate, retest_count, retest_rate


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
        input_count, pass_count, pass_rate, fail_count, fail_rate, retest_count, retest_rate = get_fpy(
            current_time_start, current_time_end)
        time_periods.append((current_time_start, current_time_end, input_count, pass_count, pass_rate, fail_count,
                             fail_rate, retest_count, retest_rate))
        current_datetime += delta

    return time_periods

#
# start = '2024-01-02 20:00:00'
# end = '2024-01-09 20:00:00'
# # station = 'TSP-E'
# # tester = '100301'
# # # # failure_message = 'FSTestProbeFsItems_OOS'
# fpy = list(get_fpy(start, end))
# key = ['input_count', 'pass_count', 'pass_rate', 'fail_count','fail_rate', 'retest_count', 'retest_rate']
# for i in range(len(fpy)):
#     print(f"{key[i]}:{fpy[i]}",end='\t')
# print()
# fpy_trend = get_fpy_time_period(start, end)
# for i in fpy_trend:
#     print(i)


def search_top_tester(start, end, result):
    # Connect to the SQLite database
    conn = sqlite3.connect('insight.db')

    # Create a cursor object
    cur = conn.cursor()
    df = pd.DataFrame()

    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        # Execute a SELECT query to retrieve data where "Test End Time" is between the time range
        cur.execute("""SELECT * FROM record 
                             WHERE "Test End Time" BETWEEN ? AND ? 
                             AND "Test Result"=? """,
                    (start_time, end_time,result))
        # Fetch all the rows that match the condition
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        top_failcount = df["Fixture ID"].value_counts()

    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_failcount


def search_top_failure(start, end, result):
    # Connect to the SQLite database
    conn = sqlite3.connect('insight.db')

    # Create a cursor object
    cur = conn.cursor()
    df = pd.DataFrame()

    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        # Execute a SELECT query to retrieve data where "Test End Time" is between the time range
        cur.execute("""SELECT * FROM record 
                            WHERE "Test End Time" BETWEEN ? AND ?
                            AND "Test Result"=?""",
                    (start_time, end_time, result))
        # Fetch all the rows that match the condition
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        top_failcount = df["Fail Message"].value_counts()

    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_failcount


def failure_count_of_tester(start, end, result, tester):
    conn = sqlite3.connect('insight.db')
    cur = conn.cursor()
    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        cur.execute("""SELECT * FROM record 
                            WHERE "Test End Time" BETWEEN ? AND ? 
                            AND "Test Result"=?
                            AND "Fixture ID"=?""",
                    (start_time, end_time, result, tester))
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        top_failcount = df['Fail Message'].value_counts()
    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return [x for x in top_failcount.items()]


def tester_count_of_failure(start, end, result, failure):
    conn = sqlite3.connect('insight.db')
    cur = conn.cursor()
    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        cur.execute("""SELECT * FROM record 
                            WHERE "Test End Time" BETWEEN ? AND ?
                            AND "Test Result"=?
                            AND "Fail Message" = ? """,
                    (start_time, end_time, result, failure))
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        top_failcount = df['Fixture ID'].value_counts()
    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_failcount


# start = '2024-01-08 20:00:00'
# end = '2024-01-09 23:59:59'
# print(search_top_tester(start, end,'RETEST'))
# print(search_top_failure(start, end, 'RETEST'))
# print(tester_count_of_failure(start, end, 'RETEST', 'FSTestProbeFsItems_OOS'))
# print(failure_count_of_tester(start, end, 'RETEST', '100101'))

# testers = ['100101', '100102', '100103', '100104', '100105', '100106', '100201', '100202', '100203', '100204',
#            '100205', '100206', '100301', '100302', '100303', '100304', '100305', '100306', '100401', '100402', '100403',
#            '100404', '100405', '100406', '100501', '100502', '100503', '100504', '100505', '100506', '100601', '100602']
# for tester in testers:
#     print(failure_count_of_tester(start, end, 'RETEST', tester))
# print(get_fpy_by_tester(start, end))
def check_for_csv_file(directory):
    csv_pattern = '*.csv'
    csv_files=glob.glob(os.path.join(directory,csv_pattern))
    return len(csv_files)


def run_analysis(path, start, end):
    if check_for_csv_file(path):
        add_from_files(path)
    else:
        print(get_fpy_by_tester(start, end))
#
# start = '2024-01-09 20:00:00'
# end = '2024-01-10 20:00:00'
# path='csv_files'
# get_fpy_by_tester(start, end)[['input_count', 'pass_count', 'pass_rate',  'retest_count', 'retest_rate','fail_msg']]