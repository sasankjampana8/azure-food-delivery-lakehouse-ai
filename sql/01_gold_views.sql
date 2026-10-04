-- Exported from the working Synapse serverless database on 2026-10-04.
-- Run once after the Gold Delta folders exist; CREATE VIEW requires absent names.
USE food_delivery_analytics;
GO

CREATE VIEW dbo.city_cuisine_pricing AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stfooddeliverylake.dfs.core.windows.net/landing/gold/city_cuisine_pricing/',
    FORMAT = 'DELTA'
) AS data;
GO

CREATE VIEW dbo.city_food_demand AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stfooddeliverylake.dfs.core.windows.net/landing/gold/city_food_demand/',
    FORMAT = 'DELTA'
) AS data;
GO

CREATE VIEW dbo.restaurant_performance AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stfooddeliverylake.dfs.core.windows.net/landing/gold/restaurant_performance/',
    FORMAT = 'DELTA'
) AS data;
GO
