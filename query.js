import {pgconnect} from "pgwire";
import _ from "lodash";
import dotenv from "dotenv";
import axios from "axios";

dotenv.config();

const api = axios.create({
	baseURL: "http://localhost:3001/",
	// baseURL: "https://services.dhis2.hispuganda.org/",
});
const query = async () => {
	const hirarchy = {
		0: "national",
		1: "region",
		2: "district",
		3: "subcounty",
		4: "facility",
	};
	// const args = process.argv.slice(2);

	const client = await pgconnect({
		user: process.env.PG_USER,
		password: process.env.PG_PASSWORD,
		hostname: process.env.PG_HOST,
		port: process.env.PG_PORT,
		database: process.env.PG_DATABASE,
	});

	try {
		const { results } = await client.query(
			`select dv.value,
  (
    select JSON_AGG(og.uid) as v
    from orgunitgroup og
      inner join orgunitgroupmembers ogm using(orgunitgroupid)
    where organisationunitid = dv.sourceid
  ) groupings,
  array_to_json(string_to_array(LTRIM(o.path, '/'), '/'))::jsonb as levels,
  de.uid as dataElement,
  cc.uid as categoryCombo,
  o.uid orgUnit,
  (
    select JSON_AGG(deco.uid)
    from dataelementcategoryoption deco
    where deco.name in(
        select unnest (string_to_array(coc.name, ', ')::text [])
      )
  ) categoryOptions
from datavalue dv
  inner join organisationunit o on(o.organisationunitid = dv.sourceid)
  inner join dataelement de using(dataelementid)
  inner join categoryoptioncombo coc using(categoryoptioncomboid)
  inner join categorycombo cc using(categorycomboid)
limit 10;`
		);
		console.log(JSON.stringify(results, null, 2));
		// for (const { rows, columns } of results) {
		// 	const data = rows.map((r) => {
		// 		return _.fromPairs(
		// 			columns.map(({ name }, index) => {
		// 				return [name, r[index]];
		// 			})
		// 		);
		// 	});
		// const all = _.chunk(data, 10000).map((chunk) => {
		// 	return api.post(`research/index?index=${args[2]}`, {
		// 		data: chunk,
		// 	});
		// });
		// const response = await Promise.all(all);
		// console.log(response);
		// }
	} catch (error) {
		console.log(error.message);
	} finally {
		client.end();
	}
};

query().then(() => console.log("Done"));
