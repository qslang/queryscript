# QueryScript

_Do more with SQL!_

Queryscript is a SQL-based language that allows you to use higher order abstractions like
variables, functions, and modules alongside SQL queries. The QueryScript compiler typechecks
your queries and then compiles away these abstractions into vanilla ANSI SQL that runs
directly against your favorite databases. QueryScript aims to do for SQL what TypeScript
did for JavaScript.

Below, we'll walk through some examples and installation instructions. For a more detailed walkthrough
of the language, check out the [QueryScript website](https://queryscript.com/).

## Example

Here's a quick example of QueryScript:

```sql
-- Variables (including relations) are namespaced and can be imported. The below queries will
-- work for any definition of `users` and `events` as long as they typecheck.
import users, events from schema;

-- You can define variables (including scalars and relations)
-- using `let <name> [<type>] = <expr>`. These expressions are
-- lazily evaluated (and potentially inlined), so there is no
-- cost to defining them.
let active_users = SELECT * FROM users WHERE active;

-- You can run queries inline to help with debugging
SELECT COUNT(*) FROM active_users;

-- You can define functions that operate over values. Just like
-- variables, functions can be inlined and pushed down into the
-- target database, or evaluated directly.
fn username(user_id bigint) {
    SELECT name FROM active_users WHERE id = user_id
}

-- Find the users with the most events
SELECT username(user_id), COUNT(*) FROM events GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- Find the users who are at risk of churn
SELECT username(user_id), MAX(ts) FROM events GROUP BY 1 HAVING MAX(ts) < NOW() - INTERVAL 1 MONTH;

-- You can also define functions over relations
fn most_popular_events(events) {
    SELECT name, COUNT(*) num FROM events ORDER BY 2 DESC
}

-- And see the most popular events globally
most_popular_events(events);

-- Or per user (this will return a sorted array of events
-- for each user)
SELECT username(user_id), most_popular_events(e) FROM events e GROUP BY 1;
```

## Installation

Right now, the only way to install QueryScript is to [build it from source](#building-from-source). We'll enable a more streamlined
install in the coming weeks.

## Building from source

To build QueryScript from source, you'll need [Git](https://git-scm.com/), [Rust](https://www.rust-lang.org/tools/install), and [Clang++](https://clang.llvm.org/).

```bash
git clone https://github.com/qscl/queryscript.git
cd queryscript
git submodule update --init
cargo build --release
```

The QueryScript compiler will be available at `target/release/qs`.

## Building development environment

If you'd like to contribute or otherwise hack on QueryScript, you'll need a few more dependencies: [Python >= 3.7](https://www.python.org/downloads/), [Python venv](https://docs.python.org/3/library/venv.html), [Git LFS](https://git-lfs.com/), [Node.js](https://nodejs.org/en/download/), and [Yarn](https://classic.yarnpkg.com/en/docs/install/).

First, run the above commands (although you can skip `cargo build --release`). Then, run

```bash
make develop
```

Beyond compiling the Rust library and `qs` binary in debug mode, this command will also:

- Setup a python package named `qsutils` inside of a virtual environment with some utility scripts for downloading test data
- Install [pre-commit](https://pre-commit.com/) hooks (inside of the virtual environment).
- Install the [VSCode](https://code.visualstudio.com/) extension's dependencies and compile it.
- Download test files via `git-lfs`

Please run `make develop` if you plan on making any contributions. You can run the test suite with `make test`.

## Background

The modern data stack has brought many of the benefits of robust software engineering to data analysis,
including source control, tests, and collaboration. QueryScript builds on this work by deeply understanding
SQL and extending the language to add views, functions, and types as first class objects _without_ having to
install anything in your database. SQL code run by QueryScript is typechecked, a code analysis technique used
by compilers like Typescript and Rust, that tells you whether your code will break without having to run it.
This has some obvious benefits — in large SQL or BI projects, you can make changes and know whether or not
you will break views without having to expensively update your warehouse.

QueryScript's deep understanding of SQL leads to even richer benefits. You can factor the SQL you write into
views or functions in your source code, which your team can freely reuse through version control. You can run
SQL queries directly against QueryScript referencing those abstractions, and it will automatically translate
them to your data warehouse. This means views are disentangled from the warehouse (no `CREATE VIEW`
statements), and two branches, users, or projects can have their own views over a dataset that without
conflicts. Your code also becomes warehouse-agnostic. The same queries will work against Snowflake, DuckDB,
or even just plain CSV files. You can test and develop completely locally, and be confident that the SQL
queries you write will run against your cloud data warehouse.

If you encode your application's data model in QueryScript, the benefits compound further. Because QueryScript schemas can run against multiple database backends, the same models that
power your application in Postgres can power your analytics and data science in your warehouse. If you
come up with an interesting view that would be useful to show in the application, you can even contribute
that _back into_ the product as a query. Ultimately, we believe in a future where analysts and developers
can collaborate directly in code.
