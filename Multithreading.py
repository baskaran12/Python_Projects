from __future__ import print_function

import pandas as pd
import cx_Oracle
import datetime
import random
import threading
from multiprocessing import Process
from threading import Thread

#cx_Oracle.init_oracle_client(lib_dir= r"C:\Users\2076284\Downloads\instantclient-basic-windows.x64-21.7.0.0.0dbru\instantclient_21_7")

pool=cx_Oracle.SessionPool("DA_USER","DA_Prod123","oridb-wc-stga.sys.comcast.net:1555/ORIONSTGA",min=10,max=10, increment=0,threaded=True, encoding="UTF-8")
def do_query(part_num):
    with pool.acquire() as conn:
        cursor = conn.cursor()
        cursor.execute("begin DV_SQL_PARALLEL_EXEC_REF(:1); end;",
                [part_num])
        conn.commit()
        print("Completed Thread : ", part_num)
        # pool.release(conn)
        # pool.close()
    endTime = datetime.datetime.today()
    print("Profiles load end time : ",endTime)

threads = []
for i in range(10):
    thread = threading.Thread(target = do_query, args = (i,))
    threads.append(thread)
    thread.start()

print("All threads started...waiting for them to complete...")
startTime = datetime.datetime.today()
print("Profiles load start time : ",startTime)
for thread in threads:
    thread.join()
#to change the no. of threads,change min and max and change range + change in code



