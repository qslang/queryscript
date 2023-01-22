export type AircraftModel {
  aircraft_model_code text,
  manufacturer text,
  model text,
  aircraft_type_id bigint,
  aircraft_engine_type_id bigint,
  aircraft_category_id bigint,
  amateur bigint,
  engines bigint,
  seats bigint,
  weight bigint,
  speed bigint,
}

export type Aircraft {
  id bigint,
  tail_num text,
  aircraft_serial text,
  aircraft_model_code text,
  aircraft_engine_code text,
  year_built bigint,
  aircraft_type_id bigint,
  aircraft_engine_type_id bigint,
  registrant_type_id bigint,
  name text,
  address1 text,
  address2 text,
  city text,
  state text,
  zip text,
  region text,
  county text,
  country text,
  certification text,
  status_code text,
  mode_s_code text,
  fract_owner text,
  last_action_date date,
  cert_issue_date date,
  air_worth_date date,
}


export type Airport {
  id bigint,
  code text,
  site_number text,
  fac_type text,
  fac_use text,
  faa_region text,
  faa_dist text,
  city text,
  county text,
  state text,
  full_name text,
  own_type text,
  longitude number,
  latitude number,
  elevation bigint,
  aero_cht text,
  cbd_dist bigint,
  cbd_dir text,
  act_date text,
  cert text,
  fed_agree text,
  cust_intl text,
  c_ldg_rts text,
  joint_use text,
  mil_rts text,
  cntl_twr text,
  major text,
}

export type Carrier {
  code text,
  name text,
  nickname text,
}

export type Flight {
  carrier text,
  origin text,
  destination text,
  flight_num text,
  flight_time bigint,
  tail_num text,
  dep_time timestamp,
  arr_time timestamp,
  dep_delay bigint,
  arr_delay bigint,
  taxi_out bigint,
  taxi_in bigint,
  distance bigint,
  cancelled text,
  diverted text,
  id2 bigint,
}

export let aircraft [Aircraft] = load('data/aircraft.parquet');
export let airports [Airport] = load('data/airports.parquet');
export let carriers [Carrier] = load('data/carriers.parquet');
export let flights [Flight] = load('data/flights.parquet');
export let aircraft_models [AircraftModel] = load('data/aircraft_models.parquet');
