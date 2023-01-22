export let cities = load('MDataFiles_Stage2/Cities.parquet');
export let conferences = load('MDataFiles_Stage2/Conferences.parquet');
export let conference_tourney_games = load('MDataFiles_Stage2/MConferenceTourneyGames.parquet');
export let game_cities = load('MDataFiles_Stage2/MGameCities.parquet');
export let massey_ordinals_thruday_128 = load('MDataFiles_Stage2/MMasseyOrdinals_thruDay128.parquet');
export let ncaa_tourney_compact_results = load('MDataFiles_Stage2/MNCAATourneyCompactResults.parquet');
export let ncaa_tourney_detailed_results = load('MDataFiles_Stage2/MNCAATourneyDetailedResults.parquet');
export let ncaa_tourney_seed_round_slots = load('MDataFiles_Stage2/MNCAATourneySeedRoundSlots.parquet');
export let ncaa_tourney_seeds = load('MDataFiles_Stage2/MNCAATourneySeeds.parquet');
export let ncaa_tourney_slots = load('MDataFiles_Stage2/MNCAATourneySlots.parquet');
export let regular_season_compact_results = load('MDataFiles_Stage2/MRegularSeasonCompactResults.parquet');
export let regular_season_detailed_results = load('MDataFiles_Stage2/MRegularSeasonDetailedResults.parquet');
export let seasons = load('MDataFiles_Stage2/MSeasons.parquet');
export let secondary_tourney_compact_results = load('MDataFiles_Stage2/MSecondaryTourneyCompactResults.parquet');
export let secondary_tourney_teams = load('MDataFiles_Stage2/MSecondaryTourneyTeams.parquet');
export let team_coaches = load('MDataFiles_Stage2/MTeamCoaches.parquet');
export let team_conferences = load('MDataFiles_Stage2/MTeamConferences.parquet');

-- There's something broken in this CSV file
-- export let team_spellings = load('MDataFiles_Stage2/MTeamSpellings.csv');

export let teams = load('MDataFiles_Stage2/MTeams.parquet');

SELECT COUNT(*) FROM teams;
