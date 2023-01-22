type Actor {
    aid text,
    url text,
    type text,
    gender text,
};

type Appearance {
    aid text,
    tid bigint,
    capacity text,
    role text,
    charid double,
    impid double,
    voice bool,
    epid bigint,
    sid bigint,
};

type Character {
    charid bigint,
    aid text,
    name text,
}

type Episode {
    sid bigint,
    epid bigint,
    aired text,
    epno bigint,
}

type Impression {
    impid bigint,
    aid text,
    name text,
}

export let actors [Actor] = load('data/actors.csv');
export let appearances [Appearance] = load('data/appearances.csv');
export let characters [Character] = load('data/characters.csv');
export let episodes [Episode] = load('data/episodes.csv');
export let impressions [Impression] = load('data/impressions.csv');
