export type User {
    id int,
    org_id int,
    name string,
}

export type Event {
    user_id int,
    description string,
    ts string,
}

export let users [User] = load('data/users.json');
export let events [Event] = load('data/events.json');
