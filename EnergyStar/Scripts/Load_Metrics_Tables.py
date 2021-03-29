
"""
Author: Alan Danque
Date:   20210304
Purpose:Optimized Data Loader from Python Underconstruction - to be shaped into a class.
Dependencies:
"""
import FastDataLoadSQLTable
import yaml
from yaml import load, dump
from pathlib import Path

def LoadMetricTables(mypath, filename):
    base_dir = Path(mypath)
    ymlfile = base_dir.joinpath(filename)
    with open(ymlfile, 'r') as stream:
        try: 
            cfg = yaml.safe_load(stream)
            PropertyList_table_name = cfg["LoadSQL_PropertyList"].get("table_name")
            PropertyList_file_path = cfg["LoadSQL_PropertyList"].get("file_path")
            PropertyList_server = cfg["LoadSQL_PropertyList"].get("server")
            PropertyList_database = cfg["LoadSQL_PropertyList"].get("database")
            PropertyList_refresh = cfg["LoadSQL_PropertyList"].get("refresh")
            PropertyList_mypath = cfg["LoadSQL_PropertyList"].get("mypath")

            BuildingAttributes_table_name = cfg["LoadSQL_BuildingAttributes"].get("table_name")
            BuildingAttributes_file_path = cfg["LoadSQL_BuildingAttributes"].get("file_path")
            BuildingAttributes_server = cfg["LoadSQL_BuildingAttributes"].get("server")
            BuildingAttributes_database = cfg["LoadSQL_BuildingAttributes"].get("database")
            BuildingAttributes_refresh = cfg["LoadSQL_BuildingAttributes"].get("refresh")
            BuildingAttributes_mypath = cfg["LoadSQL_BuildingAttributes"].get("mypath")

            EnergyStarMetrics_table_name = cfg["LoadSQL_EnergyStarMetrics"].get("table_name")
            EnergyStarMetrics_file_path = cfg["LoadSQL_EnergyStarMetrics"].get("file_path")
            EnergyStarMetrics_server = cfg["LoadSQL_EnergyStarMetrics"].get("server")
            EnergyStarMetrics_database = cfg["LoadSQL_EnergyStarMetrics"].get("database")
            EnergyStarMetrics_refresh = cfg["LoadSQL_EnergyStarMetrics"].get("refresh")
            EnergyStarMetrics_mypath = cfg["LoadSQL_EnergyStarMetrics"].get("mypath")

            WholeBuildingUsage_table_name = cfg["LoadSQL_WholeBuildingUsage"].get("table_name")
            WholeBuildingUsage_file_path = cfg["LoadSQL_WholeBuildingUsage"].get("file_path")
            WholeBuildingUsage_server = cfg["LoadSQL_WholeBuildingUsage"].get("server")
            WholeBuildingUsage_database = cfg["LoadSQL_WholeBuildingUsage"].get("database")
            WholeBuildingUsage_refresh = cfg["LoadSQL_WholeBuildingUsage"].get("refresh")
            WholeBuildingUsage_mypath = cfg["LoadSQL_WholeBuildingUsage"].get("mypath")        

        except yaml.YAMLError as exc:
            print(exc)

    # Load Staging Tables
    FastDataLoadSQLTable.bulk_csv_to_sql_load(PropertyList_table_name, PropertyList_file_path, PropertyList_server, PropertyList_database, PropertyList_refresh, PropertyList_mypath)
    FastDataLoadSQLTable.bulk_csv_to_sql_load(BuildingAttributes_table_name, BuildingAttributes_file_path, BuildingAttributes_server, BuildingAttributes_database, BuildingAttributes_refresh, BuildingAttributes_mypath)
    FastDataLoadSQLTable.bulk_csv_to_sql_load(EnergyStarMetrics_table_name, EnergyStarMetrics_file_path, EnergyStarMetrics_server, EnergyStarMetrics_database, EnergyStarMetrics_refresh, EnergyStarMetrics_mypath)
    FastDataLoadSQLTable.bulk_csv_to_sql_load(WholeBuildingUsage_table_name, WholeBuildingUsage_file_path, WholeBuildingUsage_server, WholeBuildingUsage_database, WholeBuildingUsage_refresh, WholeBuildingUsage_mypath)

    monthlymetrics_siteElectricityUseMonthly_tbl = str(WholeBuildingUsage_table_name).replace('_STAGING','_siteElectricityUseMonthly_STAGING')
    monthlymetrics_siteElectricityUseMonthly_fp = str(WholeBuildingUsage_file_path).replace('.csv','_siteElectricityUseMonthly.csv')
    FastDataLoadSQLTable.bulk_csv_to_sql_load(monthlymetrics_siteElectricityUseMonthly_tbl, monthlymetrics_siteElectricityUseMonthly_fp, WholeBuildingUsage_server, WholeBuildingUsage_database, WholeBuildingUsage_refresh, WholeBuildingUsage_mypath)
    
    monthlymetrics_siteNaturalGasUseMonthly_tbl = str(WholeBuildingUsage_table_name).replace('_STAGING','_siteNaturalGasUseMonthly_STAGING')
    monthlymetrics_siteNaturalGasUseMonthly_fp = str(WholeBuildingUsage_file_path).replace('.csv','_siteNaturalGasUseMonthly.csv')	
    FastDataLoadSQLTable.bulk_csv_to_sql_load(monthlymetrics_siteNaturalGasUseMonthly_tbl, monthlymetrics_siteNaturalGasUseMonthly_fp, WholeBuildingUsage_server, WholeBuildingUsage_database, WholeBuildingUsage_refresh, WholeBuildingUsage_mypath)    

