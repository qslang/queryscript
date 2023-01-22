export type User {
    id int,
    org_id int,
    name string,
    active bool,
}

export type Event {
    user_id int,
    description string,
    ts string,
}

export let users [User] = load('users.json');
export let events [Event] = load('events.json');
