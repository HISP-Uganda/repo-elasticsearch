select (
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
  dv.*
from datavalue dv
  inner join organisationunit o on(o.organisationunitid = dv.sourceid)
  inner join dataelement de using(dataelementid)
  inner join categoryoptioncombo coc using(categoryoptioncomboid)
  inner join categorycombo cc using(categorycomboid)
  inner join period p using(periodid)
  inner join periodtype pt using(periodtypeid)
where de.uid = ''
  and p.startdate >= ''
  and p.enddate <= '';