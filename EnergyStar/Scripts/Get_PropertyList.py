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
from pathlib import Path
import urllib.request
import ssl
import certifi
import os
import time
from datetime import date, datetime, timedelta

def Extract_PropertyList(ca_certs, startyear, url, user, pwd, cust_id, ExtPropList_outfilename):
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
    r = requests.get(u, auth=HTTPBasicAuth(user, pwd), verify=ca_certs)
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
