"""
Author: Alan Danque
Date:   20210304
Purpose:Optimized Data Loader from Python Underconstruction - to be shaped into a class.
Dependencies:
"""
from pandas import ExcelWriter
from pathlib import Path
import time
import os
import sys
import pandas as pd
import pyodbc
import contextlib
from datetime import date, datetime, timedelta
from shutil import copyfile

def bulk_csv_to_sql_load(table_name, file_path, server, database, refresh, mypath):
    start_datetime = datetime.now()
    start_time = time.time()
    print(sys.executable)

    base_dir = Path(mypath)
    appdir = Path(os.path.dirname(base_dir))
    log_dir = appdir.joinpath('Logs')
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now()
    starttime = str(datetime.now())
    file_name = os.path.basename(file_path)
    dst_path = '//'+server+'//bcptemp//'+ file_name
    copyfile(file_path, dst_path)
    
    bulkinscmd = "BULK INSERT {} FROM '{}' WITH (FIELDTERMINATOR = ',', FORMAT='CSV', ROWTERMINATOR = '\\n', FIRSTROW = 2, KEEPNULLS);"
    truncsql = f"TRUNCATE TABLE {database+'..'+table_name}"
    print(truncsql)
    errors={}
    error_out={}
    err = 0
    errors['starttime'] = str(datetime.now())

    # Test Connection Before Execution.
    try:
        contextlib.closing(pyodbc.connect(
                        'Driver={SQL Server};'
                        'Server='+ server + ';'
                        'Database=' + database + ';'
                        'Trusted_Connection=yes;'
                    )) 
    except Exception as e:
        err+=1
        ts = str(datetime.now())
        errval = "Cmd: ODBC Connection Test Failed!  Err: "+str(e)
        errors['ExecutionStatus'] = errval
        print(e)
        print(errval)

    else:
        with contextlib.closing(pyodbc.connect(
                'Driver={SQL Server};'
                'Server='+ server + ';'
                'Database=' + database + ';'
                'Trusted_Connection=yes;'
            )) as conn1:
            with contextlib.closing(conn1.cursor()) as cursor:
                print(bulkinscmd.format(table_name, dst_path))
                conn1.timeout = 500
                if refresh == 1:
                    try:
                        cursor.execute(truncsql)
                    except Exception as e:
                        err+=1
                        ts = str(datetime.now())
                        errval = "Cmd: "+ truncsql +" Err: "+str(e)
                        errors['ExecutionStatus'] = errval

                try:
                    bc = bulkinscmd.format(table_name, dst_path)
                    cursor.execute(bc)
                except Exception as e:
                    err+=1
                    ts = str(datetime.now())
                    errval = "Cmd: "+ bc +" Err: "+str(e)
                    errors['ExecutionStatus'] = errval
                else:
                    ts = str(datetime.now())
                    rowsaffected = cursor.rowcount
                    errval ="Rows Loaded: " + str(rowsaffected)
                    print(errval)
                    errors['ExecutionStatus'] = errval
            conn1.commit()
    if err > 0:
        error_out = errors
        ts = str(datetime.now())
        error_out['status'] = "Exception! Duration: %s seconds ---" % (time.time() - start_time) + " Completed at: "+ ts
    else:
        ts = str(datetime.now())
        error_out = errors
        error_out['status'] = "Success! Duration: %s seconds ---" % (time.time() - start_time) + " Completed at: "+ ts

    # Execution Logging
    error_out['src_file_path'] = file_path
    error_out['tgt_server'] = server
    error_out['tgt_database'] = database
    error_out['tgt_table_name'] = table_name
    error_out['endtime'] = str(datetime.now())
    
    execinstance = pd.DataFrame(error_out, index=[0])
    ResXlsxFile = table_name+'_DataLoad_ExecutionLog_'+ time.strftime("%Y%m%d-%H%M%S")+'.xlsx'
    ResXlsx = log_dir.joinpath(ResXlsxFile)
    writer = ExcelWriter(ResXlsx)
    execinstance.to_excel(writer, 'Results', index=0)
    writer.save()
    
    print("Status Code: %s" % str(err))
    print("Complete: --- %s seconds has passed ---" % (time.time() - start_time))
    print("Detailed Execution Log: %s" % ResXlsx)
    print(error_out)
    return error_out


