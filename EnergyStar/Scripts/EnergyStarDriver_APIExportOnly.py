

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
from pathlib import Path
import queue
import _thread
import threading
import subprocess

from multiprocessing import Pool
import time
from datetime import date, datetime, timedelta

def run_process(process):
    print(type(process))
    print(process)
    processpartlist = process.split(",")
    process = "{} {} {} {} {} {} ".format(processpartlist[0], processpartlist[1], processpartlist[2], processpartlist[3], processpartlist[4], processpartlist[5] )
    print(process)
    proc = subprocess.Popen(process)
    proc.communicate()

if __name__ == '__main__':
    mypath = sys.argv[1]
    filename = sys.argv[2]
    start_time = time.time()
    base_dir = Path(mypath)
    ymlfile = base_dir.joinpath(filename)    
    with open(ymlfile, 'r') as stream:
        try: 
            cfg = yaml.safe_load(stream)

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
            print(exc)

    # Get PropertyList
    dfprops = Get_PropertyList.Extract_PropertyList(ca_certs, startyear, url, user, pwd, cust_id, ExtPropList_outfilename)

    # Get Metrics
    # BuildingAttributes 
    # oscommand1 = '"'+venvpath+'","'+ ExtBldgAttr_pygetfname +'","'+mypath+'","'+filename+'","'+'BuildingAttributes'+'","'+ExtBldgAttr_outputpath+'"'
    
    # WholeBuildingUsage 
    oscommand2 = '"'+venvpath+'","'+ ExtWhBldgUsage_pygetfname+'","'+mypath+'","'+filename+'","'+'WholeBuildingUsage'+'","'+ExtWhBldgUsage_outputpath+'"'
    print(oscommand2)

    # EnergyStarMetrics 
    # oscommand3 = '"'+venvpath+'","'+ ExtEnrgSMetrics_pygetfname+'","'+mypath+'","'+filename+'","'+'EnergyStarMetrics'+'","'+ExtEnrgSMetrics_outputpath+'"'
    commands = [oscommand2] #commands = [oscommand1, oscommand2, oscommand3]
    pool = Pool(processes=3)
    print(commands)
    pool.map(run_process, commands)

    # Loads all 4 staging tables fast
    #Load_Metrics_Tables.LoadMetricTables(mypath, filename)
    print("EnergyStar API Integration Run Complete: --- %s total seconds has passed ---" % (time.time() - start_time) )
