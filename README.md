# Energy Star API Integration

## _Energy Star Data Download_

<a href="https://www.linkedin.com/in/alandanque"> Author: Alan Danque </a>

<a href="https://adanque.github.io/">Click here to go back to Portfolio Website </a>

![A remote image](https://adanque.github.io/assets/img/work-analytics.jpg)

API Integration with Energy Star.

## Pythonic Libraries Used in this project
- sys
- os
- yaml
- pathlib
- queue
- _thread
- threading
- subprocess
- pandas
- pyodbc
- contextlib
- shutil
- contextlib
- glob
- json
- requests
- ssl
- xml.tree.Elementree
- certifi
- urllib
- pprint
- multiprocessing
- pandasql
- time
- datetime

## Repo Folder Structure

└───EnergyStar

    ├───Configs

    ├───History

    ├───Load

    ├───Logs

    ├───Scripts

    └───Staging
	

## Python Files 

| File Name  | Description |
| ------ | ------ |
| EnergyStarDriver.py | Driver to extract and load API data |
| EnergyStarDriver_APIExportOnly.py | Driver to only extract API data |
| EnergyStarDriver_LoadSQLOnly.py | Driver to only load data |
| FastDataLoadSQLTable.py | Optimize relational table loader |
| Get_EnergyStar_BuildingAttributes_Data.py | Gathers Building Attribute Data |
| Get_EnergyStar_EnergyStarMetrics_Data.py | Gathers Energy Star Metrics Data |
| Get_EnergyStar_WholeBuildingUsage_Data.py | Gathers Whole Building Usage Data |
| Get_PropertyList.py | Gathers Property List Data |
| Load_Metrics_Tables.py | Optimized metrics loader |
| RunEnergyStarAPIIntegration.bat | Task Scheduler Call Driver Script |
| RunEnergyStarAPIIntegration_APIExportOnly.bat | Task Scheduler Call Driver Script |
| RunEnergyStarAPIIntegration_LoadSQLOnly.bat | Task Scheduler Call Driver Script |
