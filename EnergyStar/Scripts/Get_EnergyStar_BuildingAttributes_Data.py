"""
Author: Alan Danque
Date:   20210301
Purpose:Search through json file
Dependencies:
    openpyxl
    pandas
    xlrd
    pyodbc
"""
import time
import os, shutil
import glob
import numpy as np
from pathlib import Path
from os import listdir
from os.path import isfile, join
from contextlib import suppress
import pandas as pd
import pandasql as ps
import json
import pyodbc 
import sys
from pprint import pprint
import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as xml
import urllib.request
import ssl
import certifi
from pandas import ExcelWriter
from datetime import date, datetime, timedelta
import pyodbc 
import multiprocessing as mp
from threading import Thread
import yaml
from yaml import load, dump
from pathlib import Path
runningpids ={}



def init(queue):
    global idx
    idx = queue.get()

def ExtractData(dt):
    prop_id = dt['prop_id']
    pname = dt['pname']
    ledger = dt['ledger']
    clst = dt['Ext_clst']
    xlsfilename = dt['xlsfilename']
    mypath = dt['mypath']
    sysname = dt['sysname']
    ExtPropList_outfilename = dt['ExtPropList_outfilename']
    ca_certs = dt['ca_certs']
    startyear = dt['startyear']
    url = dt['url']
    user = dt['user']
    pwd = dt['pwd']
    cust_id = dt['cust_id']
    ExportData_outfilename = dt['Ext_outfilename']
    import multiprocessing
    global runningpids
    global idx
    created = multiprocessing.Process()
    current = multiprocessing.current_process()
    procid = os.getpid()
    print(sysname+" PID: "+str(procid)+ " ENG-STAR-PROPID:"+str(prop_id)+" LEDGER:" + str(ledger)+ " PROPNAME:"+ str(pname))
    runningpids[procid]="Started"
    clst = list(clst.split("~"))
    
    start_datetime = datetime.now()
    start_time = time.time()
    interval = .1

    # Create list of years starting from 2015    
    startDate = date(startyear, 1, 1)
    endDate = date.today()
    years = [year for year in range(startDate.year, endDate.year + 1)]
    meter_cfg = {}
    base_dir = Path(mypath)
    xlsfile = base_dir.joinpath(xlsfilename)
    bfilename = str(ledger) +'_'+ sysname + '.csv'
    appdir = Path(os.path.dirname(base_dir))
    stag_dir = appdir.joinpath('Staging')
    DataExportFile = stag_dir.joinpath(bfilename)

    if os.path.exists(DataExportFile):
        print("file is there before recreating!!!")

    # Read control file
    xls = pd.ExcelFile(xlsfile)
    xlsheet = xls.sheet_names
    d = pd.read_excel(xlsfile, sheet_name=xlsheet)
    df = d["Energy Star"]
    dfxml = df

    #****************************************************
    #BuildingAttributes
    TargetTableMetrics = dfxml[dfxml['Table Name'] == sysname]
    column_names = clst 
    df_TargetMetricsTransform = pd.DataFrame(columns=column_names)
    if 'year' in df_TargetMetricsTransform:
        df_TargetMetricsTransform = df_TargetMetricsTransform.drop('year', 1) # Remove the 'year' column 

    df_TargetMetricsTransform_tmp = pd.DataFrame()
    df_TargetMetricsTransform_tmp['Year'] = years
    df_TargetMetricsTransform_tmp['prop_id'] = str(prop_id)

    # Metric Level
    for windex, wrow in TargetTableMetrics.iterrows():    
        murl = wrow['MURL']
        fieldname = wrow['Field Name']
        #print(fieldname)

        metrics = [] 
        # Year Level
        for yr in years:
            #print(yr)
            u = url + '/property/' + str(prop_id) + str(murl) + str(yr) + '&month=12&measurementSystem=EPA'
            h = {'PM-Metrics': fieldname}
            r = requests.get(u, auth=HTTPBasicAuth(user, pwd), headers=h, verify=ca_certs)
            xml_root = xml.fromstring(r.text)
            #print(r.status_code)

            v_val = 0 
            if str(murl) in 'monthly':
                for monthly_metric in xml_root.iter('monthlyMetric'):
                    metric = {}
                    metric['prop_id'] = prop_id
                    metric['year_val'] = monthly_metric.attrib['year']
                    metric['month'] = monthly_metric.attrib['month']
                    metric['value'] = monthly_metric.find('value').text
                    metrics.append(metric)
            else:
                metric = {}
                metric['prop_id'] = prop_id
                metric['year_val'] = yr
                metric['month'] = '12'
                
                for v in xml_root.iter('value'):
                    v_val = v.text
                metric['value'] = v_val or "0"
            metrics.append(metric)
            dfout = pd.DataFrame(metrics)
        df_TargetMetricsTransform_tmp[fieldname] = dfout['value']

    #Add to final DataFrame
    if 'year' in df_TargetMetricsTransform_tmp:
        df_TargetMetricsTransform_tmp = df_TargetMetricsTransform_tmp.drop('year', 1) # Remove the 'year' column 
    
    df_TargetMetricsTransform_tmp['Year']=years
    #print(df_TargetMetricsTransform_tmp)
    df_TargetMetricsTransform_tmp.to_csv(DataExportFile, sep=',', index=False, mode = 'a', header=False)
    print(sysname + " Propid:" + str(prop_id) +" LEDGER:" + str(ledger)+ " PROPNAME:"+ str(pname)+ " Complete: -- %s seconds " % (time.time() - start_time) + " Process ID:" + str(procid))
    duration = (time.time() - start_time)

    #Add error handling for statusid here
    statusid = 1
    runningpids[procid]="Complete"
    return(procid, duration, statusid)


if __name__ == '__main__':
    dctparmslist = []
    
    mypath = sys.argv[1]
    filename = sys.argv[2]
    sysname = sys.argv[3]
    outputpath = sys.argv[4]    

    Extract_sysname = "Extract_"+sysname
    base_dir = Path(mypath)
    ymlfile = base_dir.joinpath(filename)
    start_time = time.time()
    manager = mp.Manager()
    n_cores = 15
    Pool = mp.Pool(n_cores)

    with open(ymlfile, 'r') as stream:
        try: 
            cfg = yaml.safe_load(stream)

            ca_certs = cfg["Get_EnergyStar"].get("ca_certs") 
            startyear = cfg["Get_EnergyStar"].get("startyear") 
            url = cfg["Get_EnergyStar"].get("url") 
            user = cfg["Get_EnergyStar"].get("user") 
            pwd = cfg["Get_EnergyStar"].get("pwd") 
            cust_id = cfg["Get_EnergyStar"].get("cust_id") 
            xlsfilename = cfg["Get_EnergyStar"].get("xlsfilename") # METRICS CONTROL FILE

            ExtPropList_outfilename = cfg["Extract_PropertyList"].get("outfilename") 

            Ext_outfilename = cfg[Extract_sysname].get("outfilename")
            Ext_clst = cfg[Extract_sysname].get("clst")
            Ext_parmlst = cfg[Extract_sysname].get("parmlst")

        except yaml.YAMLError as exc:
            print(exc)



    base_dir = Path(mypath)
    xlsfile = base_dir.joinpath(xlsfilename)
    bfilename = sysname + '.csv'
    appdir = Path(os.path.dirname(base_dir))
    outdir = Path(outputpath)
    DataExportFile = outdir.joinpath(bfilename)	

    if os.path.exists(DataExportFile):
        print("file is there")
        os.remove(DataExportFile)

    appdir = Path(os.path.dirname(base_dir))
    stag_dir = appdir.joinpath('Staging')
    stag_dir.mkdir(parents=True, exist_ok=True)

    # Remove files from staging folder 
    for filename in os.listdir(stag_dir):
        file_path = os.path.join(stag_dir, filename)
        if sysname in filename:
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))
    
    i=0
    # Read Property List to get headers for new file prep
    dfp = pd.read_csv(ExtPropList_outfilename)

    # Building Attributes
    column_names = list(Ext_clst.split("~"))
    df_TargetMetricsTransform = pd.DataFrame(columns=column_names)
    df_TargetMetricsTransform.to_csv(DataExportFile, sep=',', index=False, mode = 'a', header=True)

    funcarray = {}
    for pindex, prow in dfp.iterrows():
        prop_id = prow['id']
        ledger = prow['ledger']
        pname = prow['property']
        funcarray[prop_id] = [prop_id, pname, ledger, Ext_clst, xlsfilename, mypath, sysname, ExtPropList_outfilename, ca_certs, startyear, url, user, pwd, cust_id, Ext_outfilename]

    fcolnames = list(Ext_parmlst.split("~"))
    for pindex, prow in funcarray.items():
        dt = dict(zip(fcolnames, prow))
        dctparmslist.append(dict(dt))
    dtlen = len(dctparmslist)
    print(dtlen)
    res = Pool.map(ExtractData, dctparmslist)

    Pool.close()
    Pool.join()

    #Create output
    stag_dir_p = str(stag_dir)+'\\*'+sysname+'.csv'
    with open(DataExportFile, "ab") as outfile:
        for filename in glob.glob(stag_dir_p):
            if filename == DataExportFile:
                continue
            with open(filename, 'rb') as readfile:
                shutil.copyfileobj(readfile, outfile)

    print(runningpids)
    print(sysname+ " Complete: --- %s seconds has passed ---" % (time.time() - start_time) )
    
