type Bar {
    a number,
    b number,
    c number,
}
let foo = SELECT * FROM bar;
let bar [Bar] = load('/dev/null');
