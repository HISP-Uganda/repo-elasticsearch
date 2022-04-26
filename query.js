const { Pool } = require("pg");
// const {}= require("lodash");
const dotenv = require("dotenv");
const axios = require("axios");

dotenv.config();

const api = axios.create({
	baseURL: "http://localhost:3001/",
	// baseURL: "https://services.dhis2.hispuganda.org/",
});

const pool = new Pool({
	user: process.env.PG_USER,
	password: process.env.PG_PASSWORD,
	hostname: process.env.PG_HOST,
	port: process.env.PG_PORT,
	database: process.env.PG_DATABASE,
});
const query = async () => {
	const args = process.argv.slice(2);
	const client = await pool.connect();
	try {
		const results = await client.query(
			`select (
    select JSON_AGG(og.uid) as v
    from orgunitgroup og
      inner join orgunitgroupmembers ogm using(orgunitgroupid)
    where organisationunitid = dv.sourceid
  ) groupings,
  array_to_json(string_to_array(LTRIM(o.path, '/'), '/'))::jsonb as levels,
  de.uid as dx,
  cc.uid as co,
  o.uid as ou,
  o.hierarchylevel,
  (
    select JSON_AGG(deco.uid)
    from dataelementcategoryoption deco
    where deco.name in(
        select unnest (string_to_array(coc.name, ', ')::text [])
      )
  ) categoryOptions,
  (
    select JSON_AGG(row_to_json(t))
    from categorycombos_categories ccc
      inner join (
        select dec.categoryid,
          dec.uid,
          JSON_AGG(deco.uid)
        from dataelementcategoryoption deco
          inner join categories_categoryoptions ccc using(categoryoptionid)
          inner join dataelementcategory dec using(categoryid)
        group by dec.uid,
          dec.categoryid
      ) t using(categoryid)
    where ccc.categorycomboid = cc.categorycomboid
  ) categories,
  p.startdate,
  p.enddate,
  pt.name,
  dv.value
from datavalue dv
  inner join organisationunit o on(o.organisationunitid = dv.sourceid)
  inner join dataelement de using(dataelementid)
  inner join categoryoptioncombo coc using(categoryoptioncomboid)
  inner join categorycombo cc using(categorycomboid)
  inner join period p using(periodid)
  inner join periodtype pt using(periodtypeid)
where de.uid = '$1'
  and p.startdate >= '$2'
  and p.enddate <= '$3';`,
			[args[0], args[1], args[2]]
		);

		console.log(results);
		// for (const { rows, columns } of results) {
		// 	const data = rows.map((r) => {
		// 		return _.fromPairs(
		// 			columns.map(({ name }, index) => {
		// 				return [name, r[index]];
		// 			})
		// 		);
		// 	});
		// console.log(data);
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
