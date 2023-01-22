import schema;

import carriers from schema;
fn get_carrier(code text) {
    single(SELECT * FROM carriers c WHERE c.code = code)
}

let airports =
    SELECT *,
           concat(code, '-', full_name) AS name,
      FROM schema.airports;
fn get_airport(code: text) {
    single(SELECT * FROM airports a WHERE a.code = code)
}

import aircraft_models from schema;
fn get_aircraft_model(code: text) {
    single(SELECT * FROM aircraft a WHERE a.aircraft_model_code = code)
}

import aircraft from schema;
fn get_aircraft(tail_num: text) {
    single(SELECT * FROM aircraft a WHERE a.tail_num = tail_num)
}

let flights =
    SELECT * EXCLUDE (origin, destination),
           origin AS origin_code,
           destination AS destination_code,
      FROM schema.airports;
fn get_airport(code: text) {
    single(SELECT * FROM airports a WHERE a.code = code)
}

fn by_carrier(f: [Flight]) {
    SELECT get_carrier(carrier).nickname,
           count(*) AS flight_count,
           count(get_airport(destination_code)) AS destination_count,
           count(*) / (SELECT count(*) FROM f) * 100 AS percentage_of_flights,
      FROM f
     GROUP BY 1
}

fn by_origin(f: [Flight]) {
    SELECT get_airport(origin_code).name,
           count(*) AS flight_count,
           count(get_airport(destination_code)) AS destination_count,
           count(get_carrier(carrier)) AS carrier_count,
           count(*) / (SELECT count(*) FROM f) * 100 AS percentage_of_flights,
      FROM f
     GROUP BY 1
}

fn by_month(f: [Flight]) {
    SELECT date_trunc('month', dep_time) AS dep_month,
           count(*) AS flight_count,
      FROM f
     GROUP BY 1
}

export let flights_by_carrier = by_carrier(flights);
export let flights_by_origin = by_origin(flights);
export let flights_by_month = by_month(flights);
