def table_exists(session, name='', schema=''):
    exists = session.sql("select exists (select * from information_schema.tables where table_schema = '{}' and TABLE_NAME = '{}') as table_exists".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists