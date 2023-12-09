import psycopg2

class PGConn:
    def __init__(self, config):
        self.details = config.setup_details["psql"]
        self.conn = None

    def connection(self):
        """Return connection to PostgreSQL.  It does not need to be closed
        explicitly.  See the destructor definition below.

        """
        if self.conn is None:
            conn = psycopg2.connect(dbname=self.details["database"],
                                    host=self.details["host"],
                                    port=self.details["port"],
                                    user=self.details["user"],
                                    password=self.details["password"])
            self.conn = conn
            self.conn.autocommit = True
            
        return self.conn

    def __del__(self):
        """No need to explicitly close the connection.  It will be closed when
        the PGConn object is garbage collected by Python runtime.
        """
        print(self.conn)
        if self.conn is not None:
            self.conn.close()
        self.conn = None
        
def check_schema_exists(psql_conn, schema_name):
    sql_query=f"""
        select 
            schema_name 
        from 
            information_schema.schemata 
        where 
            schema_name='{schema_name}'
    """
    with psql_conn.cursor() as curr:
        curr.execute(sql_query)
        schema_exists = curr.fetchone()
    if schema_exists is not None:
        return True
    else:
        return False

def create_schema(psql_conn, schema_name, delete_original = False):
    comment_schema_drop="--"
    if delete_original and check_schema_exists(psql_conn, schema_name):
            comment_schema_drop=""
        
    sql_query=f"""
        {comment_schema_drop} drop schema {schema_name} cascade;
        create schema if not exists {schema_name};
    """
    with psql_conn.cursor() as curr:
        curr.execute(sql_query)