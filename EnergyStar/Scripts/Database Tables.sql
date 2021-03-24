

CREATE DATABASE [APIDataIntegration]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'APIDataIntegration', FILENAME = N'E:\SQL_DATA\APIDataIntegration.mdf' , SIZE = 1048576KB , FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'APIDataIntegration_log', FILENAME = N'S:\SQL_LOGS\APIDataIntegration_log.ldf' , SIZE = 102400KB , FILEGROWTH = 65536KB )
GO
ALTER DATABASE [APIDataIntegration] SET COMPATIBILITY_LEVEL = 140
GO
ALTER DATABASE [APIDataIntegration] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [APIDataIntegration] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [APIDataIntegration] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [APIDataIntegration] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [APIDataIntegration] SET ARITHABORT OFF 
GO
ALTER DATABASE [APIDataIntegration] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [APIDataIntegration] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [APIDataIntegration] SET AUTO_CREATE_STATISTICS ON(INCREMENTAL = OFF)
GO
ALTER DATABASE [APIDataIntegration] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [APIDataIntegration] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [APIDataIntegration] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [APIDataIntegration] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [APIDataIntegration] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [APIDataIntegration] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [APIDataIntegration] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [APIDataIntegration] SET  DISABLE_BROKER 
GO
ALTER DATABASE [APIDataIntegration] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [APIDataIntegration] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [APIDataIntegration] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [APIDataIntegration] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [APIDataIntegration] SET  READ_WRITE 
GO
ALTER DATABASE [APIDataIntegration] SET RECOVERY FULL 
GO
ALTER DATABASE [APIDataIntegration] SET  MULTI_USER 
GO
ALTER DATABASE [APIDataIntegration] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [APIDataIntegration] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [APIDataIntegration] SET DELAYED_DURABILITY = DISABLED 
GO
USE [APIDataIntegration]
GO
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = Off;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET LEGACY_CARDINALITY_ESTIMATION = Primary;
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET MAXDOP = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = On;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET PARAMETER_SNIFFING = Primary;
GO
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = Off;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET QUERY_OPTIMIZER_HOTFIXES = Primary;
GO
USE [APIDataIntegration]
GO
IF NOT EXISTS (SELECT name FROM sys.filegroups WHERE is_default=1 AND name = N'PRIMARY') ALTER DATABASE [APIDataIntegration] MODIFY FILEGROUP [PRIMARY] DEFAULT
GO




USE APIDataIntegration
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

if object_id('EnergyStar_Properties_STAGING') is not null drop table [EnergyStar_Properties_STAGING]
GO

CREATE TABLE [dbo].[EnergyStar_Properties_STAGING](
	[Energy Star Profile ID] [varchar](50) NULL,
	[Ledger] [varchar](50) NULL,
	[Property Name] [varchar](50) NULL
) ON [PRIMARY]
GO

USE APIDataIntegration
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

if object_id('EnergyStar_BuildingAttributes_STAGING') is not null drop table [EnergyStar_BuildingAttributes_STAGING]
GO

CREATE TABLE [dbo].[EnergyStar_BuildingAttributes_STAGING](
	[Yr] [varchar](50) NULL,
	[prop_id] [varchar](50) NULL,
	[id] [varchar](50) NULL,
	[nonTimeWeightedFloorArea] [varchar](50) NULL,
	[multifamilyHousingGrossFloorArea] [varchar](50) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnits] [varchar](50) NULL,
	[multifamilyHousingNumberOfBedrooms] [varchar](50) NULL,
	[multifamilyHousingNumberOfBedroomsDensity] [varchar](50) NULL,
	[parkingOpenParkingLotSize] [varchar](50) NULL,
	[parkingPartiallyEnclosedParkingGarageSize] [varchar](50) NULL,
	[parkingCompletelyEnclosedParkingGarageSize] [varchar](50) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsHighRiseSetting] [varchar](50) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsHighRiseSettingDensity] [varchar](50) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsLowRiseSetting] [varchar](50) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsLowRiseSettingDensity] [varchar](50) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsMidRiseSetting] [varchar](50) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsMidRiseSettingDensity] [varchar](50) NULL,
	[listOfAllPropertyUseTypesAtProperty] [varchar](1000) NULL,
	[largestPropertyUseType] [varchar](50) NULL,
	[largestPropertyUseTypeGFA] [varchar](50) NULL,
	[secondLargestPropertyUseType] [varchar](50) NULL,
	[secondLargestPropertyUseTypeGFA] [varchar](50) NULL,
	[thirdLargestPropertyUseType] [varchar](1000) NULL,
	[thirdLargestPropertyUseTypeGFA] [varchar](50) NULL
) ON [PRIMARY]
GO

USE APIDataIntegration
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

if object_id('EnergyStar_Metrics_STAGING') is not null drop table [EnergyStar_Metrics_STAGING]
GO

CREATE TABLE [dbo].[EnergyStar_Metrics_STAGING](
	[Yr] [smallint] NULL,
	[prop_id] [int] NULL,
	[id] [int] NULL,
	[siteTotalWN] [decimal](19, 2) NULL,
	[sourceTotalWN] [decimal](19, 2) NULL,
	[siteIntensityWN] [decimal](19, 2) NULL,
	[sourceIntensityWN] [decimal](19, 2) NULL,
	[score] [decimal](19, 2) NULL,
	[siteIntensity] [decimal](19, 2) NULL,
	[sourceIntensity] [decimal](19, 2) NULL,
	[siteTotal] [decimal](19, 2) NULL,
	[sourceTotal] [decimal](19, 2) NULL,
	[WaterScore] [varchar](250) NULL,
	[waterIntensityTotal] [decimal](19, 2) NULL,
	[directGHGEmissions] [decimal](19, 2) NULL,
	[indirectGHGEmissions] [decimal](19, 2) NULL,
	[Total GHG Emissions] [decimal](19, 2) NULL,
	[TotalGHGEmissions] [decimal](19, 2) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsDensity] [decimal](19, 2) NULL,
	[siteEnergyUseAdjustedToCurrentYear] [decimal](19, 2) NULL,
	[sourceEnergyUseAdjustedToCurrentYear] [decimal](19, 2) NULL,
	[siteIntensityAdjustedToCurrentYear] [decimal](19, 2) NULL,
	[sourceIntensityAdjustedToCurrentYear] [decimal](19, 2) NULL
) ON [PRIMARY]
GO

USE APIDataIntegration
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

if object_id('EnergyStar_WholeBuildingUsage_STAGING') is not null drop table [EnergyStar_WholeBuildingUsage_STAGING]
GO

CREATE TABLE [dbo].[EnergyStar_WholeBuildingUsage_STAGING](
	[Year] [smallint] NULL,
	[prop_id] [int] NULL,
	[Energy Star Profile ID] [int] NULL,
	[Month] [tinyint] NULL,
	[BillingPeriod] [int] NULL,
	[siteElectricityUseMonthly] [decimal](19, 2) NULL,
	[siteNaturalGasUseMonthly] [decimal](19, 2) NULL,
	[electric_kwh] [decimal](19, 6) NULL,
	[gas_therms] [decimal](19, 6) NULL
) ON [PRIMARY]
GO


-----*****-----*****-----*****-----*****-----*****-----*****-----*****-----*****-----*****

USE [ECE_DATAMART]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

if object_id('[Property List]') is not null drop table [Property List]
go
CREATE TABLE [dbo].[Property List](
	[Energy Star Profile ID] [int] NULL,
	[Ledger] [int] NULL,
	[Property Name] [varchar](50) NULL
) ON [PRIMARY]
GO

if object_id('[BuildingAttributes]') is not null drop table [BuildingAttributes]
go
CREATE TABLE [dbo].[BuildingAttributes](
	[Energy Star Profile ID] [int] NULL,
	[year] [smallint] NULL,
	[nonTimeWeightedFloorArea] [decimal](19, 2) NULL,
	[multifamilyHousingGrossFloorArea] [decimal](19, 2) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnits] [decimal](19, 2) NULL,
	[multifamilyHousingNumberOfBedrooms] [decimal](19, 2) NULL,
	[multifamilyHousingNumberOfBedroomsDensity] [decimal](19, 2) NULL,
	[parkingOpenParkingLotSize] [decimal](19, 2) NULL,
	[parkingPartiallyEnclosedParkingGarageSize] [decimal](19, 2) NULL,
	[parkingCompletelyEnclosedParkingGarageSize] [decimal](19, 2) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsHighRiseSetting] [decimal](19, 2) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsHighRiseSettingDensity] [decimal](19, 2) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsLowRiseSetting] [decimal](19, 2) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsLowRiseSettingDensity] [decimal](19, 2) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsMidRiseSetting] [decimal](19, 2) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsMidRiseSettingDensity] [decimal](19, 2) NULL,
	[listOfAllPropertyUseTypesAtProperty] [varchar](250) NULL,
	[largestPropertyUseType] [varchar](250) NULL,
	[largestPropertyUseTypeGFA] [decimal](19, 2) NULL,
	[secondLargestPropertyUseType] [varchar](250) NULL,
	[secondLargestPropertyUseTypeGFA] [decimal](19, 2) NULL,
	[thirdLargestPropertyUseType] [varchar](250) NULL,
	[thirdLargestPropertyUseTypeGFA] [decimal](19, 2) NULL
) ON [PRIMARY]
GO

if object_id('[EnergyStarMetrics]') is not null drop table [EnergyStarMetrics]
go

CREATE TABLE [dbo].[EnergyStarMetrics](
	[Energy Star Profile ID] [int] NULL,
	[weatherNormalizedSiteEnergyUse_kbtu] [decimal](19, 2) NULL,
	[weatherNormalizedSourceEnergyUse_kbtu] [decimal](19, 2) NULL,
	[siteIntensityWN] [decimal](19, 2) NULL,
	[sourceIntensityWN] [decimal](19, 2) NULL,
	[year] [smallint] NULL,
	[score] [decimal](19, 2) NULL,
	[siteIntensity] [decimal](19, 2) NULL,
	[sourceIntensity] [decimal](19, 2) NULL,
	[siteTotal] [decimal](19, 2) NULL,
	[sourceTotal] [decimal](19, 2) NULL,
	[WaterScore] [decimal](19, 2) NULL,
	[waterIntensityTotal] [decimal](19, 2) NULL,
	[directGHGEmissions] [decimal](19, 2) NULL,
	[indirectGHGEmissions] [decimal](19, 2) NULL,
	[totalGHGEmissions] [decimal](19, 2) NULL,
	[multifamilyHousingNumberOfResidentialLivingUnitsDensity] [decimal](19, 2) NULL,
	[siteEnergyUseAdjustedToCurrentYear] [decimal](19, 2) NULL,
	[sourceEnergyUseAdjustedToCurrentYear] [decimal](19, 2) NULL,
	[siteIntensityAdjustedToCurrentYear] [decimal](19, 2) NULL,
	[sourceIntensityAdjustedToCurrentYear] [decimal](19, 2) NULL
) ON [PRIMARY]
GO

if object_id('[WholeBuildingUsage]') is not null drop table [WholeBuildingUsage]
go
CREATE TABLE [dbo].[WholeBuildingUsage](
	[Energy Star Profile ID] [int] NULL,
	[Year] [smallint] NULL,
	[Month] [tinyint] NULL,
	[BillingPeriod] [int] NULL,
	[electricity_kbtu] [decimal](19, 2) NULL,
	[naturalgas_kbtu] [decimal](19, 2) NULL,
	[electric_kwh] [decimal](19, 6) NULL,
	[naturalgas_therms] [decimal](19, 6) NULL
) ON [PRIMARY]
GO


/*
USE TARGETDB

TRUNCATE TABLE TARGETDB..[Property List]
INSERT INTO TARGETDB..[Property List]
	--SELECT * FROM [APIDataIntegration]..[EnergyStar_Properties_STAGING]
	--Have to fix the python script.
	SELECT [energy star profile id], [Ledger] AS Ledger, [Property Name] AS [Property Name] FROM [APIDataIntegration]..[EnergyStar_Properties_STAGING]



TRUNCATE TABLE TARGETDB..[BuildingAttributes] 
INSERT INTO TARGETDB..[BuildingAttributes]
	SELECT 
		 prop_id AS [Energy Star Profile ID]
		,Yr AS [year]
		,nonTimeWeightedFloorArea
		,multifamilyHousingGrossFloorArea
		,multifamilyHousingNumberOfResidentialLivingUnits
		,multifamilyHousingNumberOfBedrooms
		,multifamilyHousingNumberOfBedroomsDensity
		,parkingOpenParkingLotSize
		,parkingPartiallyEnclosedParkingGarageSize
		,parkingCompletelyEnclosedParkingGarageSize
		,multifamilyHousingNumberOfResidentialLivingUnitsHighRiseSetting
		,multifamilyHousingNumberOfResidentialLivingUnitsHighRiseSettingDensity
		,multifamilyHousingNumberOfResidentialLivingUnitsLowRiseSetting
		,multifamilyHousingNumberOfResidentialLivingUnitsLowRiseSettingDensity
		,multifamilyHousingNumberOfResidentialLivingUnitsMidRiseSetting
		,multifamilyHousingNumberOfResidentialLivingUnitsMidRiseSettingDensity
		,listOfAllPropertyUseTypesAtProperty
		,largestPropertyUseType
		,largestPropertyUseTypeGFA
		,secondLargestPropertyUseType
		,secondLargestPropertyUseTypeGFA
		,thirdLargestPropertyUseType
		,thirdLargestPropertyUseTypeGFA
	FROM [APIDataIntegration]..[EnergyStar_BuildingAttributes_STAGING]

truncate table TARGETDB..[EnergyStarMetrics]
insert into TARGETDB..[EnergyStarMetrics]
SELECT 
	 prop_id AS [Energy Star Profile ID]
	,siteTotalWN AS [weatherNormalizedSiteEnergyUse_kbtu]
	,sourceTotalWN AS [weatherNormalizedSourceEnergyUse_kbtu]
	,siteIntensityWN
	,sourceIntensityWN
	,yr AS [year]
	,score
	,siteIntensity
	,sourceIntensity
	,siteTotal
	,sourceTotal
	,WaterScore
	,waterIntensityTotal
	,directGHGEmissions
	,indirectGHGEmissions
	--,[Total GHG Emissions] AS [totalGHGEmissions]
	,TotalGHGEmissions
	,multifamilyHousingNumberOfResidentialLivingUnitsDensity
	,siteEnergyUseAdjustedToCurrentYear
	,sourceEnergyUseAdjustedToCurrentYear
	,siteIntensityAdjustedToCurrentYear
	,sourceIntensityAdjustedToCurrentYear
FROM [APIDataIntegration]..[EnergyStar_Metrics_STAGING]

truncate table TARGETDB..[WholeBuildingUsage]
insert into TARGETDB..[WholeBuildingUsage]
select
	 prop_id AS [Energy Star Profile ID]
	,[Year] AS [Year]
	,[Month] AS [Month]
	,cast([Year] AS varchar(4))+cast([Month] AS varchar(4)) AS [BillingPeriod]
	,isnull(siteElectricityUseMonthly, 0) AS [electricity_kbtu] 
	,isnull(siteNaturalGasUseMonthly, 0) AS [naturalgas_kbtu] 
	,isnull(electric_kwh, 0) AS [electric_kwh] 
	,isnull(gas_therms, 0) AS [naturalgas_therms] 
FROM [APIDataIntegration]..[EnergyStar_WholeBuildingUsage_STAGING]



*/


select count(*) from [APIDataIntegration]..[EnergyStar_Properties_STAGING]
select count(*) from [APIDataIntegration]..[EnergyStar_BuildingAttributes_STAGING]
select count(*) from [APIDataIntegration]..[EnergyStar_WholeBuildingUsage_STAGING]
select count(*) from [APIDataIntegration]..[EnergyStar_Metrics_STAGING]

select count(*) from TARGETDB..[Property List]
select count(*) from TARGETDB..[WholeBuildingUsage]
select count(*) from TARGETDB..[EnergyStarMetrics]
select count(*) from TARGETDB..[BuildingAttributes] 



