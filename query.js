const { Pool } = require("pg");
const _ = require("lodash");
const dotenv = require("dotenv");
const axios = require("axios");
const { parseISO, format } = require("date-fns");

const generateDaily = (startDate) => {
	const date = parseISO(startDate);
	const day = format(date, "yyyyMMdd");
	const week = format(date, "yyyy[W]cc");
	const month = format(date, "yyyyMM");
	const quarter = format(date, "yyyyQQQ");
	const year = format(date, "yyyy");

	return { day, week, month, quarter, year };
};

const generateWeekly = (startDate) => {
	const date = parseISO(startDate);
	const week = format(date, "yyyy[W]cc");
	const month = format(date, "yyyyMM");
	const quarter = format(date, "yyyyQQQ");
	const year = format(date, "yyyy");

	return { week, month, quarter, year };
};
const generateMonthly = (startDate) => {
	const date = parseISO(startDate);
	console.log(startDate,typeof date);
	// const month = format(date, "yyyyMM");
	// const quarter = format(date, "yyyyQQQ");
	// const year = format(date, "yyyy");

	return {  };
};
const generateQuarterly = (startDate) => {
	const date = parseISO(startDate);
	const quarter = format(date, "yyyyQQQ");
	const year = format(date, "yyyy");

	return { quarter, year };
};

const generateYearly = (startDate) => {
	const date = parseISO(startDate);
	const year = format(date, "yyyy");

	return { year };
};

const getPeriod = (type, startDate) => {
	console.log(startDate, type);
	const all = {
		// Daily: generateDaily(startDate),
		// Weekly: generateWeekly(startDate),
		Monthly: generateMonthly(startDate),
		// Quarterly: generateQuarterly(startDate),
		// Yearly: generateYearly(startDate),
	};
	return all[type] || {};
};

dotenv.config();

const api = axios.create({
	baseURL: "https://services.dhis2.hispuganda.org/",
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
	console.log(args);

	const client = await pool.connect();
	try {
		const { rows } = await client.query(
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
where de.uid = $1
  and p.startdate >= $2
  and p.enddate <= $3;`,
			[args[0], args[1], args[2]]
		);

		const data = rows.map((r) => {
			const { categories, levels, ...others } = r;
			const processed = categories.map(({ uid, json_agg }) => {
				return [uid, _.intersection(json_agg, r.categoryoptions)[0]];
			});
			return {
				...others,
				..._.fromPairs(processed),
				..._.fromPairs(levels.map((l, i) => [`level${i + 1}`, l])),
				...getPeriod(r.name, r.startdate),
			};
		});
		// console.log(data);
		const all = _.chunk(data, 10000).map((chunk) => {
			return api.post(`research/index?index=${args[3]}`, {
				data: chunk,
			});
		});
		const response = await Promise.all(all);
		console.log(response);
	} catch (error) {
		console.log(error.message);
	} finally {
		client.end();
	}
};

query().then(() => console.log("Done"));
