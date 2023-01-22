export type AircraftModel {
    number_value bigint,
}

-- This should throw a runtime error
export let aircraft_models [AircraftModel] = load('data/aircraft_models.parquet');

SELECT MAX(number_value) FROM aircraft_models;
