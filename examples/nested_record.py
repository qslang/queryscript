# This file tests a few examples using the datafusion python library.

import datafusion
from datafusion import col
import pyarrow as pa


def run_query(ctx, query):
    print("----- Query -----")
    print(query)
    print("----- Results -----")
    try:
        print(ctx.sql(query).collect()[0].to_pylist())
    except Exception as e:
        print(e)
    print("-----"*10)


# create a context
ctx = datafusion.SessionContext()

data = [{"a": 1, "b": {"c": 2}}, {"a": 2, "b": {"c": 3, "d": 1}}]
rb = pa.RecordBatch.from_pylist(data)
ctx.register_record_batches("test", [[rb]])

run_query(ctx, "select array_agg(test.a) from test")
run_query(ctx, "select test.b from test")
run_query(ctx, "select array_agg(b) from test")
run_query(ctx, "select array_agg(test.b) from test")
run_query(ctx, "select array_agg(test.b) from test")

run_query(ctx, "select array_agg(test) from test")

# This is snowflake style (you have to do something like array_agg(object_create(test.*))
run_query(ctx, "select array_agg(test.*) from test")
run_query(ctx, "select array_agg(*) from test")
