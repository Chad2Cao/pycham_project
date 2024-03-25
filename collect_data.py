############################################################################
#
#将xlsx文件中列‘数据信息’转化为一个dataframe,
#获取一列中信息tolist()，遍历list，json.load()将其Json格式转化成-->字典，并最终append到一个新的list.
#最后将Dict_list转化为dataframe.然后用to_sql将dataframe存储到数据库。
#
#
############################################################################
import os
import sqlite3
import pandas as pd
import res, dva, tsp

folder_path = "xlsx_files"

# Create a connection object to the database
conn = sqlite3.connect('mes.db')

# Loop through each file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".xlsx"):
        file_path = os.path.join(folder_path, filename)
        try:
            # Open the xlsx file ,return a dataframe
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

            # Convert stop_time to datetime format if it is not already
            df['stop_time'] = pd.to_datetime(df['stop_time'])
            # df['start_time'] = pd.to_datetime(df['start_time'])
            # Calculate the test_time
            # df['test_time'] = (df['stop_time'] - df['start_time']).dt.total_seconds()
            # Drop the start_time column
            # df = df.drop(['start_time'], axis=1)

            # Store the DataFrame to the database
            df.to_sql('record', conn, if_exists='append', index=False)
        except Exception as e:
            print(e)
        finally:
            os.remove(file_path)

# Close the connection
conn.close()
