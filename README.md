# Arb dashboard query repo

This repo contains queries for the arbitrum dashboard

## Overall Section
This is the main landing page metrics
Aggregating across the entire arbitrum landscape

**to ponder:** we should see if shaving of non program data makes sense. like we dont want ltipp uniswap to contribute new users to stip program duh.  

## Comparison Section
This is section that will distribute metrics across different groups protocol aggregations like:
* Program : STIP, Backfund, LTIPP
* Sector: DEX, Lending, Perps etc
* Grant amount : 
* Protocol size : 

## Protocol Section
This is for the Individual Protocol page. 
These metrics will be filtered for each protocol to display them

### 1. Overall

All queries to be run as it is and data downloaded

### 2. Action segmentation

All queries to be run as it is and data downloaded

### 3. User segmentation

For each segment, each template must be run.

* each template has two params:
    1. segmentation query `segment_query`
    2. default fall back value `fallback_value`

* each segment outputs `user` and `segment`
* each segment contains `default_value` to be passed
