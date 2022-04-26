const { Pool } = require("pg");
const _ = require("lodash");
const dotenv = require("dotenv");
const axios = require("axios");
const { format } = require("date-fns");

const opd = require("./opd.json");

const q = `select (
    select JSON_AGG(og.uid) as v
    from orgunitgroup og
      inner join orgunitgroupmembers ogm using(orgunitgroupid)
    where organisationunitid = dv.sourceid
  ) groupings,
  array_to_json(string_to_array(LTRIM(o.path, '/'), '/'))::jsonb as levels,
  de.uid as dx,
  coc.uid as co,
	coca.uid as ao,
	cc.uid as c,
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
  dv.*
from datavalue dv
  inner join organisationunit o on(o.organisationunitid = dv.sourceid)
  inner join dataelement de using(dataelementid)
  inner join categoryoptioncombo coc using(categoryoptioncomboid)
	inner join categoryoptioncombo coca on(coca.categoryoptioncomboid = dv.attributeoptioncomboid)
  inner join categorycombo cc using(categorycomboid)
  inner join period p using(periodid)
  inner join periodtype pt using(periodtypeid)
where de.uid in ($1)
  and p.startdate >= $2
  and p.enddate <= $3;`;

const generateDaily = (startDate) => {
	const day = format(startDate, "yyyyMMdd");
	const week = format(startDate, "yyyy'W'cc");
	const month = format(startDate, "yyyyMM");
	const quarter = format(startDate, "yyyyQQQ");
	const year = format(startDate, "yyyy");

	return { day, week, month, quarter, year };
};

const generateWeekly = (startDate) => {
	const week = format(startDate, "yyyy'W'cc");
	const month = format(startDate, "yyyyMM");
	const quarter = format(startDate, "yyyyQQQ");
	const year = format(startDate, "yyyy");

	return { week, month, quarter, year };
};
const generateMonthly = (startDate) => {
	const month = format(startDate, "yyyyMM");
	const quarter = format(startDate, "yyyyQQQ");
	const year = format(startDate, "yyyy");

	return { month, quarter, year };
};
const generateQuarterly = (startDate) => {
	const quarter = format(startDate, "yyyyQQQ");
	const year = format(startDate, "yyyy");

	return { quarter, year };
};

const generateYearly = (startDate) => {
	const year = format(startDate, "yyyy");

	return { year };
};

const getPeriod = (type, startDate) => {
	const all = {
		Daily: generateDaily(startDate),
		Weekly: generateWeekly(startDate),
		Monthly: generateMonthly(startDate),
		Quarterly: generateQuarterly(startDate),
		Yearly: generateYearly(startDate),
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

	const client = await pool.connect();
	try {
		for (const element of _.chunk(opd, 20)) {
			const { rows } = await client.query(q, [
				element.join(","),
				args[0],
				args[1],
			]);
			const data = rows.map((r) => {
				const {
					categories,
					levels,
					dataelementid,
					periodid,
					sourceid,
					categoryoptioncomboid,
					attributeoptioncomboid,
					value,
					...others
				} = r;
				const processed = categories.map(({ uid, json_agg }) => {
					return [uid, _.intersection(json_agg, r.categoryoptions)[0]];
				});
				return {
					id: `${[
						dataelementid,
						periodid,
						sourceid,
						categoryoptioncomboid,
						attributeoptioncomboid,
					].join("")}`,
					...others,
					..._.fromPairs(processed),
					..._.fromPairs(levels.map((l, i) => [`level${i + 1}`, l])),
					...getPeriod(r.name, r.startdate),
					value: Number(value),
				};
			});
			const all = _.chunk(data, 10000).map((chunk) => {
				return api.post(`wal/index?index=${args[2]}`, {
					data: chunk,
				});
			});
			const response = await Promise.all(all);
			console.log(response);
		}
	} catch (error) {
		console.log(error.message);
	} finally {
		client.end();
	}
};

query().then(() => console.log("Done"));
