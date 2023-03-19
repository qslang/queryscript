let structs = load('struct.json');
select * from structs;
select * from structs where b = {c: 3, d: null};
select * from structs where b = {"c": 3, "d": null};

let foo = 1;
{"a": foo};
{foo: 1};
