# USGS River Data Sync

Script to sync data from USGS instantaneous water service.

## Algorithm

1. Rivers and states are fetched from the service url
2. Rivers are grouped by state abbreviation
3. Water reports are fetched and normalized for each state from USGS
4. Dictionary holds river id's / new flows
5. SQL insert statement is formed by iterating over river id / new flows dictionary
6. Connect to postgres DB and execute insert statement

## Setup

1. Install dependencies

   `npm i`

2. Copy and fill environment variables

   `cp .env.sample .env`

3. Run script

   `node index.js`
