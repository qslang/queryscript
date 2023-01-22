export let aircraft = load('data/aircraft.parquet');
export let airports = load('data/airports.parquet');
export let carriers = load('data/carriers.parquet');
export let flights = load('data/flights.parquet');
export let aircraft_models = load('data/aircraft_models.parquet');

select * from aircraft order by id asc limit 10;
select * from airports order by id asc limit 10;
select * from carriers order by code asc limit 10;
select * from flights order by id2 asc limit 10;
select * from aircraft_models order by aircraft_model_code asc limit 10;
