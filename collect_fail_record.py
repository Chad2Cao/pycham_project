import glob
import logging
import os
import sqlite3
import warnings
from datetime import datetime

import pandas as pd
from matplotlib import pyplot as plt
from tqdm import tqdm

# 忽略警告
warnings.filterwarnings('ignore')

# 设置日志记录器
logging.basicConfig(filename='insight_fail_record.log', level=logging.INFO)


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
    cursor.execute("""CREATE TABLE IF NOT EXISTS fail_record (
                   'SerialNumber' TEXT,
                   'Test Pass/Fail Status' TEXT,
                   'EndTime' TIMESTAMP,
                   'Version' TEXT,
                   'List of Failing Tests' TEXT,
                   'CARRIER_PN' TEXT,
                   'FIXTURE_ID' TEXT,
                   'CARRIER_TOTAL_TEST' INTEGER,
                   'CARRIER_UNIT_FAIL' INTEGER,
                   'Category' TEXT,
                   'Sub Category' TEXT,
                   'Sub Sub Category' TEXT)""")
    conn.commit()


@db_operation
def insert_data(conn, data):
    data.to_sql('fail_record', conn, if_exists='append', index=False)
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
        config_path = '/Users/chad/PycharmProjects/pythonProject/config/Config-File_D95x-TSP_0809.csv'
        print(f"Processing {file_path}")
        try:
            # Open the csv file
            data = pd.read_csv(str(file_path), header=1, na_values=['NA'], dtype={'FIXTURE_ID': str})
            config = pd.read_csv(config_path)
            data['CARRIER_TOTAL_TEST'] = data['CARRIER_TOTAL_TEST'].fillna(0)
            # Drop the first 5 rows
            data.drop(range(0, 5), inplace=True)
            # Convert the columns to int
            data['CARRIER_TOTAL_TEST'] = data['CARRIER_TOTAL_TEST'].fillna(0)
            data['CARRIER_TOTAL_TEST'] = data['CARRIER_TOTAL_TEST'].astype(int)
            data['CARRIER_UNIT_FAIL'] = data['CARRIER_UNIT_FAIL'].fillna(0)
            data['CARRIER_UNIT_FAIL'] = data['CARRIER_UNIT_FAIL'].astype(int)

            # Drop the columns
            column_to_delete = ['Site', 'Product', 'Special Build Name', 'Special Build Description', 'Unit Number',
                                'Station ID', 'StartTime', 'fixture_id']
            data.drop(column_to_delete, axis=1, inplace=True)
            # 筛选出满足条件的行
            rows_to_delete = data[data['Test Pass/Fail Status'] == 'PASS'].index
            # 删除这些行
            data.drop(rows_to_delete, inplace=True)
            # reset the index
            data.reset_index(drop=True, inplace=True)
            category = pd.Series(name='Category')
            sub_category = pd.Series(name='Sub Category')
            sub_sub_category = pd.Series(name='Sub Sub Category')

            for i in range(len(data)):
                fail_key = data['List of Failing Tests'][i].split(';')[0].replace(' ', '^^')
                if fail_key in config['Key'].tolist():
                    index = config['Key'].tolist().index(fail_key)
                    category[i] = config['Category'][index]
                    sub_category[i] = config['Sub Category'][index]
                    sub_sub_category[i] = config['Sub Sub Category'][index]
            data['Category'] = category
            data['Sub Category'] = sub_category
            data['Sub Sub Category'] = sub_sub_category

            if not is_table_exists('insight.db', 'fail_record'):
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


# #
# path = 'csv_files/fail_record'
# add_from_files(path)


# create_table()
@db_operation
def get_records(conn, sql, start_time, end_time):
    cursor = conn.cursor()
    cursor.execute(sql, (start_time, end_time))
    result = cursor.fetchall()
    return result


@db_operation
def get_records_by_tester(conn, sql, start_time, end_time, tester):
    cursor = conn.cursor()
    cursor.execute(sql, (start_time, end_time, tester))
    result = cursor.fetchall()
    return result


def search_top_fail_subcategory(start, end):
    # Connect to the SQLite database
    conn = sqlite3.connect('insight.db')

    # Create a cursor object
    cur = conn.cursor()
    top_fail_category = pd.Series(name='Sub Category')

    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    try:
        # Execute a SELECT query to retrieve data where "Test End Time" is between the time range
        cur.execute("""SELECT * FROM fail_record 
                             WHERE "EndTime" BETWEEN ? AND ? """,
                    (start_time, end_time))
        # Fetch all the rows that match the condition
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(fail_record)")
        column_names = [i[1] for i in cur.fetchall()]
        # Create a dataframe from the rows
        df = pd.DataFrame(rows, columns=column_names)
        # drop true failed SerialNumber
        fail_items = get_fail_sn(start_time, end_time)
        drop_list = []
        for sn in fail_items:
            drop_list.append(sn[0])
        df.drop(df[df['SerialNumber'].isin(drop_list)].index, axis=0, inplace=True)
        # Drop the duplicate rows
        df.drop_duplicates(subset='SerialNumber', inplace=True)
        top_fail_category = df["Sub Category"].value_counts()

    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_fail_category


def get_input_count(start_time, end_time):
    # conn = sqlite3.connect('insight.db')
    sql_query = f'SELECT COUNT(*) FROM record WHERE "Test End Time" BETWEEN? AND? '
    result = get_records(sql_query, start_time, end_time)
    input_count = result[0][0]
    return input_count


def get_fail_sn(start, end):
    sql_query = f"""SELECT "Serial Number" FROM record 
                    WHERE "Test End Time" BETWEEN? AND? 
                    AND "Test Result" = 'FAIL' """
    result = get_records(sql_query, start, end)

    return result


def check_for_csv_file(directory):
    csv_pattern = '*.csv'
    csv_files = glob.glob(os.path.join(directory, csv_pattern))
    return len(csv_files)


def tester_count_of_failure(start, end, sub_category):
    conn = sqlite3.connect('insight.db')
    cur = conn.cursor()
    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    top_fail_count = pd.Series(name='FIXTURE_ID')
    try:
        cur.execute("""SELECT * FROM fail_record 
                            WHERE "EndTime" BETWEEN ? AND ?
                            AND "Sub Category" = ? """,
                    (start_time, end_time, sub_category))
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(fail_record)")
        column_names = [i[1] for i in cur.fetchall()]
        # Create a DataFrame object
        df = pd.DataFrame(rows, columns=column_names)
        # drop true failed SerialNumber
        fail_items = get_fail_sn(start_time, end_time)
        drop_list = []
        for sn in fail_items:
            drop_list.append(sn[0])
        df.drop(df[df['SerialNumber'].isin(drop_list)].index, axis=0, inplace=True)

        # drop duplicate SerialNumber
        df.drop_duplicates(subset='SerialNumber', inplace=True)
        top_fail_count = df['FIXTURE_ID'].value_counts()
    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_fail_count


def carrier_count_of_failure(start, end, sub_category):
    """
    :param start:
    :param end:
    :param sub_category:
    :return: the carrier sn top fail (value_count)
    """
    conn = sqlite3.connect('insight.db')
    cur = conn.cursor()
    # Define the time range
    start_time = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    top_fail_count = pd.Series(name='CARRIER_PN')
    try:
        cur.execute("""SELECT * FROM fail_record 
                            WHERE "EndTime" BETWEEN ? AND ?
                            AND "Sub Category" = ? """,
                    (start_time, end_time, sub_category))
        rows = cur.fetchall()
        cur.execute(f"PRAGMA table_info(fail_record)")
        column_names = [i[1] for i in cur.fetchall()]

        df = pd.DataFrame(rows, columns=column_names)
        # drop true failed SerialNumber
        fail_items = get_fail_sn(start_time, end_time)
        drop_list = []
        for sn in fail_items:
            drop_list.append(sn[0])
        df.drop(df[df['SerialNumber'].isin(drop_list)].index, axis=0, inplace=True)
        df.drop_duplicates(subset='SerialNumber', inplace=True)

        top_fail_count = df['CARRIER_PN'].value_counts()
    except Exception as e:
        print(e)
        print("fail to  search")
    finally:
        # Close the cursor and the connection
        cur.close()
        conn.close()
    return top_fail_count


start = '2024-01-09 20:00:00'
end = '2024-01-16 20:00:00'


sub_category = search_top_fail_subcategory(start, end).head(6).to_frame()
sub_category['rate'] = sub_category['count']/get_input_count(start, end)*100
sub_category.to_csv('out/top_6_fail_item.csv', index=True, header=True)

print(get_input_count(start, end))
print(sub_category)
# start = '2024-01-09 20:00:00'
# end = '2024-01-16 20:00:00'
# total_input=105980
#                          count      rate
# Sub Category
# FSProbe Cal                357  0.336856
# FSProbe Test               248  0.234006
# DisplayPowerOn             234  0.220796
# FailedToFindMtDevice       172  0.162295
# Digital OS Test_DPOS       156  0.147198
# FailToReceiveDriftAlarm    120  0.113229
def plot_tester_count_by_subcategory(start, end):
    # 创建2x3子图网格
    fig, axs = plt.subplots(2, 3, figsize=(12, 8))
    limit = 80
    angle = 90  # 旋转角度，可以设置为0、45、90、-45等值
    sub_category = search_top_fail_subcategory(start, end).head(6).to_frame()
    sub_category['rate'] = sub_category['count'] / get_input_count(start, end) * 100
    sub_category.to_csv('out/top_fail_category.csv', index=True, header=True)
    failures = sub_category.index
    # print(sub_category)
    # print(tester_count_of_failure(start, end, failures[0]).values)
    x = []
    y = []
    for i in range(6):
        x.append(tester_count_of_failure(start, end, failures[i]).index)
        y.append(tester_count_of_failure(start, end, failures[i]).values)
        axs[i // 3, i % 3].bar(x[i], y[i], width=0.6, alpha=0.5, color='green')
        axs[i // 3, i % 3].set_title(failures[i])
        axs[i // 3, i % 3].set_ylim(0, limit)
        axs[i // 3, i % 3].set_xticklabels([])
        for j, v in enumerate(y[i]):
            axs[i // 3, i % 3].text(j - 0.3, v + 1.3, x[i][j], rotation=angle, fontsize=8)

    # 调整子图之间的间距
    plt.tight_layout()
    # 保存、显示图表

    plt.savefig('out/tester_distribution_on_fail_items.png')
    plt.show()


def plot_carrier_count_by_subcategory(start, end):
    # 创建2x3子图网格
    fig, axs = plt.subplots(2, 3, figsize=(12, 8))
    limit = 40
    angle = 90  # 旋转角度，可以设置为0、45、90、-45等值
    sub_category = search_top_fail_subcategory(start, end).head(6).to_frame()
    sub_category['rate'] = sub_category['count'] / get_input_count(start, end) * 100
    sub_category.to_csv('out/top_fail_category.csv', index=True, header=True)
    failures = sub_category.index
    # print(sub_category)
    # print(tester_count_of_failure(start, end, failures[0]).values)
    x = []
    y = []
    for i in range(6):
        x.append(carrier_count_of_failure(start, end, failures[i]).head(20).index)
        y.append(carrier_count_of_failure(start, end, failures[i]).head(20).values)
        axs[i // 3, i % 3].bar(x[i], y[i], width=0.6, alpha=0.5, color='orange')
        axs[i // 3, i % 3].set_title(failures[i])
        axs[i // 3, i % 3].set_ylim(0, limit)
        axs[i // 3, i % 3].set_xticklabels([])
        for j, v in enumerate(y[i]):
            axs[i // 3, i % 3].text(j - 0.3, v + 1.3, x[i][j], rotation=angle, fontsize=8)

    # 调整子图之间的间距
    plt.tight_layout()
    # 保存、显示图表

    plt.savefig('out/carrier_distribution_on_fail_items.png')
    plt.show()

#
plot_tester_count_by_subcategory(start, end)
plot_carrier_count_by_subcategory(start, end)

