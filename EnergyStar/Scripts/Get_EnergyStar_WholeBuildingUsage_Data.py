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
import math
import time
import os, shutil
import glob
import numpy as np
from pathlib import Path
from os import listdir
from os.path import isfile, join, dirname, abspath
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
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from sys import path
import sys
pyAlertPath= "E:\\pyAlerts" 
path.append(pyAlertPath)
curpath = sys.path[0]
basepath = dirname(curpath)
import pyAlert

"""
# pyAlert USAGE
subj="SENDING TEST EMAIL SUBJ"
msg="SENDING TEST EMAIL MSG"
sender="adanque@eqr.com"
emailgroup="ALAN_TEST"
send = pyAlert.sendmail(subj, msg, sender, emailgroup)
"""

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
    wenv = dt['wenv']
    sender = dt['sender']
    emailgroup = dt['emailgroup']
    hname = os.environ['COMPUTERNAME']
    starttimev = time.time()
    errors={}
    error_out={}
    err = 0
    errors['starttime'] = str(datetime.now())
    import multiprocessing
    global runningpids
    global idx
    created = multiprocessing.Process()
    current = multiprocessing.current_process()
    #print("running", current.name, current._identity)
    #print("created", created.name, created._identity)
    procid = os.getpid()
    print(sysname+" PID: "+str(procid)+ " ENG-STAR-PROPID:"+str(prop_id)+" LEDGER:" + str(ledger)+ " PROPNAME:"+ str(pname))
    runningpids[procid]="Started"
    clst = list(clst.split("~"))
    
    start_datetime = datetime.now()
    start_time = time.time()
    interval = .1
    # ENV
    #print(sys.executable)
    
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
    xlsfilename_monthly_siteElectricityUseMonthly = str(DataExportFile).replace('.csv','_siteElectricityUseMonthly.csv')
    xlsfilename_monthly_siteNaturalGasUseMonthly = str(DataExportFile).replace('.csv','_siteNaturalGasUseMonthly.csv') 

    if os.path.exists(DataExportFile):
        print("file is there before recreating!!!")
    #    os.remove(DataExportFile)
    
    # Read control file
    xls = pd.ExcelFile(xlsfile)
    xlsheet = xls.sheet_names
    d = pd.read_excel(xlsfile, sheet_name=xlsheet)
    df = d["Energy Star"]
    dfxml = df

    #****************************************************
    #
    TargetTableMetrics = dfxml[dfxml['Table Name'] == sysname]
    column_names = clst 
    df_TargetMetricsTransform = pd.DataFrame(columns=column_names)
    #print(column_names)
    if 'year' in df_TargetMetricsTransform:
        df_TargetMetricsTransform = df_TargetMetricsTransform.drop('year', 1) # Remove the 'year' column 

    df_TargetMetricsTransform_tmp = pd.DataFrame()
    mdf_TargetMetricsTransform_tmp = pd.DataFrame()

    # Metric Level
    monthlymetrics_siteElectricityUseMonthly = []
    monthlymetrics_siteNaturalGasUseMonthly = []
    for windex, wrow in TargetTableMetrics.iterrows():    
        murl = wrow['MURL']
        fieldname = wrow['Field Name']
        props = []
        metrics = [] 
        yearvals = []
        monthvals = []
        mprops = []
        mmetrics = [] 
        myearvals = []
        mmonthvals = []
        # Year Level
        for yr in years:
            u = url + '/property/' + str(prop_id) + str(murl) + str(yr) + '&month=12&measurementSystem=EPA'
            h = {'PM-Metrics': fieldname}
            #r = requests.get(u, auth=HTTPBasicAuth(user, pwd), headers=h, verify=ca_certs)
            r = requests.Session()
            retries = Retry(total=10, backoff_factor=1, status_forcelist=[502, 503, 504, 500])
            r.mount('https://', HTTPAdapter(max_retries=retries))
            try:
                r = requests.get(u, auth=HTTPBasicAuth(user, pwd), headers=h, verify=ca_certs)
                #print(r.status_code)
                r.raise_for_status()
            except requests.exceptions.HTTPError as errhttp:   
                if str(murl) != 'nan':
                    errval = "Http Error:"+ str(errhttp)
                    errors['HTTP_Error'] = errval
                    print(errors)
                    StatusCode +=1
            except requests.exceptions.ConnectionError as errConn:    
                if str(murl) != 'nan':
                    errval = "Error Connectint:"+ str(errConn)
                    errors['HTTP_Connection_Error'] = errval
                    print(errors)
                    StatusCode +=1
            except requests.exceptions.Timeout as errtout:
                if str(murl) != 'nan':
                    errval = "Timeout Error:"+ str(errtout)
                    errors['HTTP_Timeout_Error'] = errval
                    print(errors)
                    StatusCode +=1
            except requests.exceptions.TooManyRedirects as errRedir:
                if str(murl) != 'nan':
                    errval = "HTTP_TooMany_Redirects:"+ str(errRedir)
                    errors['HTTP_TooMany_Redirect_Error'] = errval
                    print(errors)
                    StatusCode +=1
            except requests.exceptions.RequestException as e:
                if str(murl) != 'nan':
                    errval = "Except:"+ str(e)
                    errors['Exception'] = errval
                    print(errors)
                    StatusCode +=1
                    raise SystemExit(e)
            if str(murl) != 'nan':
                eval_http_ret_status = r.status_code
            else:
                # Force Skip if the murl is nan
                eval_http_ret_status = 200
            print(eval_http_ret_status)
            """
            # Section # Extract_PropertyList
            """
            if eval_http_ret_status != 200 and murl_test != 'nan':
                errors['Duration'] = "Seconds:"+str((time.time() - starttimev))
                erroutv1 = flatten(errors)
                erroutv2 = iterdict(erroutv1)
                msgout= "\n".join(['='.join(i) for i in erroutv2.items()]) 
                msg = str(msgout)
                subj="EnergyStar API Integration WholeBuilding Exception - ! on Server: "+str(hname)+" : "+str(wenv)
                send = pyAlert.sendmail(subj, msg, sender, emailgroup) 
           
            xml_root = xml.fromstring(r.text)
            #print(r.status_code)

            v_val = 0 
            if 'monthly' in str(murl) and fieldname =="siteElectricityUseMonthly":
                for monthly_metric in xml_root.iter('monthlyMetric'):
                    mmetric = {}
                    mmetric['prop_id'] = prop_id or "0"
                    mmetric['year'] = monthly_metric.attrib['year']
                    mmetric['month'] = monthly_metric.attrib['month']
                    mmetric[fieldname] = monthly_metric.find('value').text or "0"
                    monthlymetrics_siteElectricityUseMonthly.append(mmetric)
            elif 'monthly' in str(murl) and fieldname =="siteNaturalGasUseMonthly":
                for monthly_metric in xml_root.iter('monthlyMetric'):
                    mmetric = {}
                    mmetric['prop_id'] = prop_id or "0"
                    mmetric['year'] = monthly_metric.attrib['year']
                    mmetric['month'] = monthly_metric.attrib['month']
                    mmetric[fieldname] = monthly_metric.find('value').text or "0"
                    monthlymetrics_siteNaturalGasUseMonthly.append(mmetric)
            else:
                metric = {}
                props.append(prop_id)
                yearvals.append(yr)
                monthvals.append('12')
                
                for v in xml_root.iter('value'):
                    v_val = v.text
                metric['value'] = v_val or "0"
                
            metrics.append(metric)
            dfout = pd.DataFrame(metrics)
        df_TargetMetricsTransform_tmp[fieldname] = dfout['value']

    # Monthly Metrics        
    df_monthlymetrics_siteElectricityUseMonthly = pd.DataFrame(monthlymetrics_siteElectricityUseMonthly)
    print("df_monthlymetrics_siteElectricityUseMonthly")
    for col in df_monthlymetrics_siteElectricityUseMonthly.columns:
        print(col)
    df_monthlymetrics_siteElectricityUseMonthly.to_csv(xlsfilename_monthly_siteElectricityUseMonthly, sep=',', index=False, mode = 'a', header=False)
    
    
    df_monthlymetrics_siteNaturalGasUseMonthly = pd.DataFrame(monthlymetrics_siteNaturalGasUseMonthly)
    print("df_monthlymetrics_siteNaturalGasUseMonthly")
    for col in df_monthlymetrics_siteNaturalGasUseMonthly.columns:
        print(col)
    df_monthlymetrics_siteNaturalGasUseMonthly.to_csv(xlsfilename_monthly_siteNaturalGasUseMonthly, sep=',', index=False, mode = 'a', header=False)
    
    # Annual Metrics        
    df_TargetMetricsTransform_tmp['prop_id'] = props
    df_TargetMetricsTransform_tmp['Year'] = yearvals
    df_TargetMetricsTransform_tmp['Month'] = monthvals
    df_TargetMetricsTransform_tmp2 = df_TargetMetricsTransform_tmp.reindex(columns = column_names)
    print("df_TargetMetricsTransform_tmp2")
    for col in df_TargetMetricsTransform_tmp2.columns:
        print(col)    
    df_TargetMetricsTransform_tmp2.to_csv(DataExportFile, sep=',', index=False, mode = 'a', header=False)
    print(sysname + " Propid:" + str(prop_id) +" LEDGER:" + str(ledger)+ " PROPNAME:"+ str(pname)+ " Complete: -- %s seconds " % (time.time() - start_time) + " Process ID:" + str(procid))
    duration = (time.time() - start_time)
    statusid = 1
    runningpids[procid]="Complete"
    return(procid, duration, statusid)


def flatten(d): 
    out = {} 
    for key, val in d.items(): 
        if isinstance(val, dict): 
            val = [val] 
        if isinstance(val, list): 
            for subdict in val: 
                deeper = flatten(subdict).items() 
                out.update({key + '_' + key2: val2 for key2, val2 in deeper}) 
        else: 
            out[key] = val 
    return out

def iterdict(d):
    for k, v in d.items():
        if isinstance(v, dict):
            iterdict(v)
        else:
            if type(v) == int or isinstance(v, pathlib.PurePath):
                v = str(v)
            d.update({k: v})
    return d

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

            emailgroup = cfg["Get_EnergyStar"].get("EmailExceptionGroup")                        
            sender = cfg["Get_EnergyStar"].get("EmailSender")                        
            wenv = cfg["Get_EnergyStar"].get("Environment") 

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

    #stag_dir = base_dir.joinpath('Staging')
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

    DataExportFile_xlsfilename_monthly_siteElectricityUseMonthly = str(DataExportFile).replace('.csv','_siteElectricityUseMonthly.csv')
    #column_names = list("prop_id, year, month, siteElectricityUseMonthly".split(","))
    df_TargetMetricsTransform = pd.DataFrame(columns=['prop_id', 'year', 'month', 'siteElectricityUseMonthly'])
    #df_TargetMetricsTransform.to_csv(DataExportFile_xlsfilename_monthly_siteElectricityUseMonthly, sep=',', index=False, mode = 'a', header=True)
    df_TargetMetricsTransform.to_csv(DataExportFile_xlsfilename_monthly_siteElectricityUseMonthly, index=False)

    DataExportFile_xlsfilename_monthly_siteNaturalGasUseMonthly = str(DataExportFile).replace('.csv','_siteNaturalGasUseMonthly.csv')
    #column_names = list("prop_id, year, month, siteNaturalGasUseMonthly".split(","))
    df_TargetMetricsTransform = pd.DataFrame(columns=['prop_id', 'year', 'month', 'siteNaturalGasUseMonthly'])
    #df_TargetMetricsTransform.to_csv(DataExportFile_xlsfilename_monthly_siteNaturalGasUseMonthly, sep=',', index=False, mode = 'a', header=True)
    df_TargetMetricsTransform.to_csv(DataExportFile_xlsfilename_monthly_siteNaturalGasUseMonthly, index=False)	

    # WholeBuilding Attributes
    # Header Files
    column_names = list(Ext_clst.split("~"))
    df_TargetMetricsTransform = pd.DataFrame(columns=column_names)
    df_TargetMetricsTransform.to_csv(DataExportFile, sep=',', index=False, mode = 'a', header=True)

    funcarray = {}
    for pindex, prow in dfp.iterrows():
        prop_id = prow['id']
        ledger = prow['ledger']
        pname = prow['property']
        funcarray[prop_id] = [prop_id, pname, ledger, Ext_clst, xlsfilename, mypath, sysname, ExtPropList_outfilename, ca_certs, startyear, url, user, pwd, cust_id, Ext_outfilename, wenv, sender, emailgroup]

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

    stag_dir_p = str(stag_dir)+'\\*'+sysname+'_siteElectricityUseMonthly.csv'
    with open(DataExportFile_xlsfilename_monthly_siteElectricityUseMonthly, "ab") as outfile:
        for filename in glob.glob(stag_dir_p):
            if filename == DataExportFile_xlsfilename_monthly_siteElectricityUseMonthly:
                continue
            with open(filename, 'rb') as readfile:
                shutil.copyfileobj(readfile, outfile)
                
    stag_dir_p = str(stag_dir)+'\\*'+sysname+'_siteNaturalGasUseMonthly.csv'
    with open(DataExportFile_xlsfilename_monthly_siteNaturalGasUseMonthly, "ab") as outfile:
        for filename in glob.glob(stag_dir_p):
            if filename == DataExportFile_xlsfilename_monthly_siteNaturalGasUseMonthly:
                continue
            with open(filename, 'rb') as readfile:
                shutil.copyfileobj(readfile, outfile)

    print(runningpids)
    print(sysname+ " Complete: --- %s seconds has passed ---" % (time.time() - start_time) )
    
