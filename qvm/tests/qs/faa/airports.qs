export Airport, airports from schema;

export fn by_state(a1 [Airport]) {
    SELECT state,
           count(1) AS airport_count
      FROM a1
     WHERE state is not null
     GROUP BY state
     ORDER BY state
}

by_state(airports);

export fn by_facility_type(a2 [Airport]) {
    SELECT fac_type,
           count(1) AS airport_count,
           avg(elevation) AS avg_elevation
      FROM a2
     GROUP BY fac_type
     ORDER BY fac_type
}

by_facility_type(airports);

export fn by_region_dashboard(a3 [Airport]) {
    SELECT faa_region,
           count(1) AS airport_count,
           by_facility_type(array_agg(a3)) AS by_facility_type,
           by_state(array_agg(a3)) AS by_state_shape_map
      FROM a3
     GROUP BY faa_region
     ORDER BY faa_region
}

export let airports_by_region_dashboard = by_region_dashboard(airports);

airports_by_region_dashboard;
