require("dotenv").config();
const pg = require("pg");
const { Client } = pg;
const {
  MANUAL_RIVERS,
  SERVICE_BASEURL,
  USGS_URL,
  DEFAULT_PERIOD,
  DATABASE_URL,
} = process.env;

const client = new Client({
  connectionString: DATABASE_URL,
});

async function getRiver(uuid) {
  return fetch(`${SERVICE_BASEURL}/api/v1/rivers/${uuid}`)
    .then((res) => {
      if (!res.ok) {
        throw res;
      }
      return res.json();
    })
    .then((data) => data)
    .catch((errorRes) => {
      console.error(`Something went wrong, error with code ${errorRes.status}`);
    });
}

async function getRivers() {
  return fetch(`${SERVICE_BASEURL}/api/v1/rivers`)
    .then((res) => {
      if (!res.ok) {
        throw res;
      }
      return res.json();
    })
    .then((data) => data)
    .catch((errorRes) => {
      console.error(`Something went wrong, error with code ${errorRes.status}`);
    });
}

async function getUsgsWaterReport(siteCodes, period = DEFAULT_PERIOD) {
  // Fetches water data from USGS. In JSON format.
  // Queries all the site codes over the period.
  // Parameter code will fetch only discharge for now
  return fetch(
    `${USGS_URL}?format=json,1.1&sites=${siteCodes}&period=${period}&parameterCd=00060`
  )
    .then((res) => {
      if (!res.ok) {
        throw res;
      }
      return res.json();
    })
    .then((data) => data)
    .catch((errorRes) => {
      console.error(`Something went wrong, error with code ${errorRes.status}`);
    });
}

function parseWaterReportResultsForSiteCodes(results, riverSiteCodeIdLookup) {
  /*
    {
      '<river uuid>': [{
          timestamp: '12345',
          discharge: '123.4',
          riverId: '123'
        }]
    }
  */
  const riverWithFlowsDict = {};
  for (const entry of results.value.timeSeries) {
    const siteCode = entry?.sourceInfo?.siteCode?.[0]?.value;
    const unit = entry?.variable?.unit?.unitCode;
    const values = entry?.values?.[0].value.reduce((acc, v) => {
      const jsDate = new Date(v.dateTime);
      const minutes = jsDate.getMinutes();

      // Only include on the hour data
      if (minutes === 0) {
        acc = [
          ...acc,
          {
            timestamp: jsDate.toUTCString(),
            value: parseFloat(v.value).toFixed(2),
            unit,
          },
        ];
      }

      return acc;
    }, []);

    const riverId = riverSiteCodeIdLookup[siteCode];
    riverWithFlowsDict[riverId] = values;
  }

  return riverWithFlowsDict;
}

function createSQLInsertStatement(riverIdsFlowsDict) {
  const insertStatementPrefix =
    "INSERT INTO water_reports (timestamp, discharge, river_id) VALUES";
  const values = Object.entries(riverIdsFlowsDict).reduce(
    (acc, [riverId, flows]) => {
      const formattedFlows = flows.map(
        (flow) => `('${flow.timestamp}', ${flow.value}, '${riverId}')`
      );

      if (formattedFlows.length > 0) {
        acc = `${acc.length > 0 ? `${acc},` : acc}${formattedFlows.join(",")}`;
      }
      return acc;
    },
    ""
  );

  return `${insertStatementPrefix} ${values}`;
}

async function syncRivers(rivers) {
  const stateSiteCodesDict = {};
  const riverSiteCodeIdDict = {};

  for (const river of rivers) {
    stateSiteCodesDict[river.stateAbbr] = [
      ...(stateSiteCodesDict[river.stateAbbr] || []),
      river.siteCode,
    ];
    riverSiteCodeIdDict[river.siteCode] = river.uuid;
  }

  // Dictionary with keys being river ids
  // Values are an array of flows for a given river
  let riverFlowsDict = {};
  // Iterate over each key (state) in stateSiteCodesDict
  for (const abbr in stateSiteCodesDict) {
    // Water reports for the site codes
    const waterReports = await getUsgsWaterReport(stateSiteCodesDict[abbr]);

    riverFlowsDict = {
      ...riverFlowsDict,
      ...parseWaterReportResultsForSiteCodes(waterReports, riverSiteCodeIdDict),
    };
  }

  const combinedSqlInsertStatement = createSQLInsertStatement(riverFlowsDict);

  await client.connect();

  try {
    await client.query(combinedSqlInsertStatement);
  } catch (err) {
    console.log(err);
  }

  await client.end();
}

async function main() {
  const manualRiverUuids = MANUAL_RIVERS?.split(",") || [];

  // If there are manual rivers, sync those instead of all rivers
  if (manualRiverUuids.length) {
    let rivers = [];
    for (const manualRiverUuid of manualRiverUuids) {
      const river = await getRiver(manualRiverUuid);
      rivers = [...rivers, river];
    }
    syncRivers(rivers);
  } else {
    const rivers = await getRivers();
    syncRivers(rivers);
  }
}

main();
