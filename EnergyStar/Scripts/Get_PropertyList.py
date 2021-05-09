#####
# Author:   Alan Danque
# Date:   
# Purpose:  Energy Star API
# Version:
#####
import pandas as pd
import sys
from pprint import pprint
import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as xml
import sys
from datetime import date
import pathlib
from pathlib import Path
import urllib.request
import ssl
import certifi
import os
import time
from datetime import date, datetime, timedelta
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
def Extract_PropertyList(ca_certs, startyear, url, user, pwd, cust_id, ExtPropList_outfilename, emailgroup, sender, wenv):
    start_datetime = datetime.now()
    start_time = time.time()
    interval = .1
    print(sys.executable)

    # Get Years since 2015
    startDate = date(startyear, 1, 1)
    endDate = date.today()
    years = [year for year in range(startDate.year, endDate.year + 1)]
    print(years)
    meter_cfg = {}
    properties = []
    props = []
    u = url + '/account/' + str(cust_id) + '/property/list'
    hname = os.environ['COMPUTERNAME']
    errors={}
    error_out={}
    err = 0
    errors['starttime'] = str(datetime.now())
    StatusCode = 0
    r = requests.Session()
    retries = Retry(total=10, backoff_factor=1, status_forcelist=[502, 503, 504, 500])
    r.mount('https://', HTTPAdapter(max_retries=retries))
    try:
        r = requests.get(u, auth=HTTPBasicAuth(user, pwd), verify=ca_certs)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errhttp:    
        errval = "Http Error:"+ str(errhttp)
        errors['HTTP_Error'] = errval
        print(errors)
        StatusCode +=1
    except requests.exceptions.ConnectionError as errConn:    
        errval = "Error Connectint:"+ str(errConn)
        errors['HTTP_Connection_Error'] = errval
        print(errors)
        StatusCode +=1
    except requests.exceptions.Timeout as errtout:
        errval = "Timeout Error:"+ str(errtout)
        errors['HTTP_Timeout_Error'] = errval
        print(errors)
        StatusCode +=1
    except requests.exceptions.TooManyRedirects as errRedir:
        errval = "HTTP_TooMany_Redirects:"+ str(errRedir)
        errors['HTTP_TooMany_Redirect_Error'] = errval
        print(errors)
        StatusCode +=1
    except requests.exceptions.RequestException as e:
        errval = "Except:"+ str(e)
        errors['Exception'] = errval
        print(errors)
        StatusCode +=1
        raise SystemExit(e)

    eval_http_ret_status = r.status_code
    print(eval_http_ret_status)
    """
    # Section # Extract_PropertyList
    """
    if eval_http_ret_status != 200:
        errors['Duration'] = "Seconds:"+str((time.time() - starttimev))
        erroutv1 = flatten(errors)
        erroutv2 = iterdict(erroutv1)
        msgout= "\n".join(['='.join(i) for i in erroutv2.items()]) 
        msg = str(msgout)
        subj="EnergyStar API Integration Exception - ! on Server: "+str(hname)+" : "+str(wenv)
        send = pyAlert.sendmail(subj, msg, sender, emailgroup) 
    
    xml_root = xml.fromstring(r.text)

    for prop in xml_root.iter('link'):
        properties.append(prop.attrib['id'])
    for prop in xml_root.iter('link'):
        props.append(prop.attrib['hint'])

    # Extract property lists
    i = 0
    df = pd.DataFrame(columns=['id', 'ledger', 'property'])
    for prop in xml_root.iter('link'):
        i +=1
        pid = prop.attrib['id']
        ledger = prop.attrib['hint'][-5:]
        pname = prop.attrib['hint'][0:-8]
        df.loc[i] = [str(pid)] + [str(ledger)] + [str(pname)] 
    #filename = 'c:\\alan\EnergyStar\\Propertylist.csv'
    if os.path.exists(ExtPropList_outfilename):
        print("file is there")
        os.remove(ExtPropList_outfilename)

    df.to_csv(ExtPropList_outfilename, sep=',', index=False)
    print("PropertyList Complete: --- %s seconds has passed ---" % (time.time() - start_time))
    return df

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