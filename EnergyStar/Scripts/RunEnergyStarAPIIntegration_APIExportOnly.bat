::Author: Alan Danque
::Date:	  20210317
::Purpose:EnergyStar API Integration

e:
cd\
cd EnergyStar
cd Scripts
python -m EnergyStarDriver_APIExportOnly "E://EnergyStar//Configs//" "Config.yaml" > E:\EnergyStar\Logs\RunEnergyStarAPIIntegration_APIExportOnly_%date:~4,2%%date:~7,2%%date:~10,4%_%time:~0,2%%time:~3,2%.log
