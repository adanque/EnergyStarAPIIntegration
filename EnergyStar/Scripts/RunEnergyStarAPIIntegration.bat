::Author: Alan Danque
::Date:	  20210317
::Purpose:EnergyStar API Integration

ren e:\EnergyStar\Load\*.csv *.csv_Prior_%date:~4,2%%date:~7,2%%date:~10,4%_%time:~0,2%%time:~3,2% & move e:\EnergyStar\Load\*.csv* e:\EnergyStar\History\
e:
cd\
cd EnergyStar
cd Scripts
python -m EnergyStarDriver "E://EnergyStar//Configs//" "Config.yaml" > E:\EnergyStar\Logs\RunEnergyStarAPIIntegration_%date:~4,2%%date:~7,2%%date:~10,4%_%time:~0,2%%time:~3,2%.log
