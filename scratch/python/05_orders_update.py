from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

from utils import table_exists

import json

def create_orders_table(session):
    _ = session.sql("CREATE TABLE IF NOT EXISTS ORDERS LIKE ORDERS_V").collect()
    _ = session.sql("ALTER TABLE ORDERS ADD COLUMN META_UPDATED_AT TIMESTAMP").collect()

def create_orders_stream(session):
    _ = session.sql("CREATE STREAM IF NOT EXISTS ORDERS_STREAM ON TABLE ORDERS \
                    SHOW_INITIAL_ROWS = TRUE;").collect()

def merge_order_updates(session):
    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE').collect()

    source = session.table('ORDERS_V_STREAM')
    target = session.table('ORDERS')

    cols_to_update = {c: source[c] for c in source.schema.names if "METADATA" not in c}
    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    updates = {**cols_to_update, **metadata_col_to_update}

    # merge into DIM_CUSTOMER
    target.merge(source, target['ORDER_DETAIL_ID'] == source['ORDER_DETAIL_ID'], \
                        [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL').collect()

    session.table('ORDERS').limit(5).show()

def main():
    with open('creds.json') as f:
        connection_params = json.load(f)

    try:
        session.close()
    except:
        pass

    session = Session.builder.configs(connection_params).create()

    session.use_database("HOL_DB")
    session.use_warehouse("HOL_WH")
    session.use_schema('HARMONIZED')

    if not table_exists(session, schema='HARMONIZED', name='ORDERS'):
        create_orders_table(session)
        create_orders_stream(session)

    merge_order_updates(session)

if __name__ == "__main__":
    main()