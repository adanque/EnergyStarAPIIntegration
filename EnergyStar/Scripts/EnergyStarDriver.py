

"""
Author: Alan Danque
Date:   20210304
Purpose:Optimized Data Loader from Python Underconstruction - to be shaped into a class.
"""
import sys
import Load_Metrics_Tables
import Get_PropertyList
import os
import yaml
from yaml import load, dump
import pathlib
from pathlib import Path
import queue
import _thread
import threading
import subprocess

from multiprocessing import Pool
import time
from datetime import date, datetime, timedelta
import os
from sys import path
import sys
from os.path import dirname, abspath
pyAlertPath= "E:\\pyAlerts" 
path.append(pyAlertPath)
curpath = sys.path[0]
basepath = dirname(curpath)
import pyAlert
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

"""
# pyAlert USAGE
subj="SENDING TEST EMAIL SUBJ"
msg="SENDING TEST EMAIL MSG"
sender="adanque@eqr.com"
emailgroup="ALAN_TEST"
send = pyAlert.sendmail(subj, msg, sender, emailgroup)
"""

def run_process(process):
    print(type(process))
    print(process)
    processpartlist = process.split(",")
    process = "{} {} {} {} {} {} ".format(processpartlist[0], processpartlist[1], processpartlist[2], processpartlist[3], processpartlist[4], processpartlist[5] )
    print(process)
    proc = subprocess.Popen(process)
    proc.communicate()

if __name__ == '__main__':
    hname = os.environ['COMPUTERNAME']
    starttimev = time.time()
    errors={}
    error_out={}
    err = 0
    errors['starttime'] = str(datetime.now())
    
    mypath = sys.argv[1]
    filename = sys.argv[2]
    start_time = time.time()
    base_dir = Path(mypath)
    ymlfile = base_dir.joinpath(filename)    
    with open(ymlfile, 'r') as stream:
        try: 
            cfg = yaml.safe_load(stream)
            
            emailgroup = cfg["Get_EnergyStar"].get("EmailExceptionGroup")                        
            sender = cfg["Get_EnergyStar"].get("EmailSender")                        
            wenv = cfg["Get_EnergyStar"].get("Environment")             

            venvpath = cfg["Get_EnergyStar"].get("venvpath") 
            ca_certs = cfg["Get_EnergyStar"].get("ca_certs") 
            startyear = cfg["Get_EnergyStar"].get("startyear") 
            url = cfg["Get_EnergyStar"].get("url") 
            user = cfg["Get_EnergyStar"].get("user") 
            pwd = cfg["Get_EnergyStar"].get("pwd") 
            cust_id = cfg["Get_EnergyStar"].get("cust_id") 
            xlsfilename = cfg["Get_EnergyStar"].get("xlsfilename") # METRICS CONTROL FILE
            ExtPropList_outfilename = cfg["Extract_PropertyList"].get("outfilename") 

            ExtBldgAttr_pygetfname = cfg["Extract_BuildingAttributes"].get("pygetfname")
            ExtWhBldgUsage_pygetfname = cfg["Extract_WholeBuildingUsage"].get("pygetfname")
            ExtEnrgSMetrics_pygetfname = cfg["Extract_EnergyStarMetrics"].get("pygetfname")

            ExtBldgAttr_outputpath = cfg["Extract_BuildingAttributes"].get("outputpath")
            ExtWhBldgUsage_outputpath = cfg["Extract_WholeBuildingUsage"].get("outputpath")
            ExtEnrgSMetrics_outputpath = cfg["Extract_EnergyStarMetrics"].get("outputpath")			

        except yaml.YAMLError as exc:
            """
            # Section 1: YAML
            """
            errval = "Except:"+ str(exc)
            errors['Exception'] = errval
            print(exc)
            subj="Chatmeter API Integration Exception - Section 1: YAML! on Server: "+str(hname)+" : "+str(wenv)
            errors['Duration'] = "Seconds:"+str((time.time() - starttimev))
            erroutv1 = flatten(errors)
            erroutv2 = iterdict(erroutv1)
            msgout= "\n".join(['='.join(i) for i in erroutv2.items()]) 
            msg = str(msgout)
            send = pyAlert.sendmail(subj, msg, sender, emailgroup)

    # Get PropertyList
    dfprops = Get_PropertyList.Extract_PropertyList(ca_certs, startyear, url, user, pwd, cust_id, ExtPropList_outfilename, emailgroup, sender, wenv)

    # Get Metrics
    # BuildingAttributes
    oscommand1 = '"'+venvpath+'","'+ ExtBldgAttr_pygetfname +'","'+mypath+'","'+filename+'","'+'BuildingAttributes'+'","'+ExtBldgAttr_outputpath+'"'
    
    # WholeBuildingUsage
    oscommand2 = '"'+venvpath+'","'+ ExtWhBldgUsage_pygetfname+'","'+mypath+'","'+filename+'","'+'WholeBuildingUsage'+'","'+ExtWhBldgUsage_outputpath+'"'

    # EnergyStarMetrics
    oscommand3 = '"'+venvpath+'","'+ ExtEnrgSMetrics_pygetfname+'","'+mypath+'","'+filename+'","'+'EnergyStarMetrics'+'","'+ExtEnrgSMetrics_outputpath+'"'
    commands = [oscommand1, oscommand2, oscommand3]
    #commands = [oscommand2]
    pool = Pool(processes=3)
    print(commands)
    pool.map(run_process, commands)

    # Loads all 4 staging tables fast
    Load_Metrics_Tables.LoadMetricTables(mypath, filename)
    print("EnergyStar API Integration Run Complete: --- %s total seconds has passed ---" % (time.time() - start_time) )

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
