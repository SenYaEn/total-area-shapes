"""
@name: db_objects\create_objects.sql
@author: jan.strocki@hotmail.co.uk

This module contains scripts to create Delta Lake objects the pipelines in this project need.
Usually this step would be automated and be part of infra release pipeline. For the POC this
is done manually.
"""

-- Creating databases for each of layer of the Medallion architecture.
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

-- A single bronze table for streamed events
CREATE TABLE IF NOT EXISTS bronze.event (
    event_id        STRING      NOT NULL,
    event_type      STRING      NOT NULL,
    event_sub_type  STRING      NOT NULL,
    value           STRING      NOT NULL,
    loaded_at       TIMESTAMP   NOT NULL
) USING DELTA;

-- A separate Silver layer table for each type of shape
CREATE TABLE IF NOT EXISTS silver.rectangle (
    rectangle_event_id  STRING      NOT NULL,
    event_id            STRING      NOT NULL,
    width               INT         NOT NULL,
    height              INT         NOT NULL,
    area                DECIMAL     NOT NULL,
    loaded_at           TIMESTAMP   NOT NULL
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.triangle (
    triangle_event_id   STRING      NOT NULL,
    event_id            STRING      NOT NULL,
    width               INT         NOT NULL,
    height              INT         NOT NULL,
    area                DECIMAL     NOT NULL,
    loaded_at           TIMESTAMP   NOT NULL
)
USING DELTA;


CREATE TABLE IF NOT EXISTS silver.circle (
    circle_event_id STRING          NOT NULL,
    event_id        STRING          NOT NULL,
    radius          INT             NOT NULL,
    area            DECIMAL(10, 2)  NOT NULL,
    loaded_at       TIMESTAMP       NOT NULL,
)
USING DELTA;


-- Gold layer for this POC has a view only
CREATE OR REPLACE VIEW gold.daily_shape_stats_vw AS (

    -- Calculate daily statistics for rectangles
    WITH rectangles_daily_stats AS 
    (
        SELECT CAST(loaded_at AS DATE)              AS loadedDate,
               COUNT(DISTINCT rectangle_event_id)   AS total_shapes,
               SUM(area)                            AS total_area
        FROM silver.rectangle
        GROUP BY CAST(loaded_at AS DATE)
    ),

    -- Calculate daily statistics for triangles
    triangles_daily_stats AS 
    (
        SELECT CAST(loaded_at AS DATE)              AS loadedDate,
               COUNT(DISTINCT triangle_event_id)    AS total_shapes,
               SUM(area)                            AS total_area
        FROM silver.triangle
        GROUP BY CAST(loaded_at AS DATE)
    ),

    -- Calculate daily statistics for circles
    circles_daily_stats AS 
    (
        SELECT CAST(loaded_at AS DATE)          AS loadedDate,
               COUNT(DISTINCT circle_event_id)  AS total_shapes,
               SUM(area) AS total_area
        FROM silver.circle
        GROUP BY CAST(loaded_at AS DATE)
    ),

    -- Combine statistics from rectangles, triangles, and circles into one unionized dataset
    unionized_daily_stats AS 
    (
        SELECT * FROM rectangles_daily_stats
        UNION
        SELECT * FROM triangles_daily_stats
        UNION
        SELECT * FROM circles_daily_stats
    )

    -- Final aggregation of the daily statistics across all shapes
    SELECT loadedDate,
           SUM(total_shapes) AS numberOfShapes,
           SUM(total_area) AS totalArea
    FROM unionized_daily_stats
    GROUP BY loadedDate
);