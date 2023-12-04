import psycopg2

class PGConn:
    def __init__(self, config):
        self.setup_details = config["psql"]
        self.conn = None

    def connection(self):
        """Return connection to PostgreSQL.  It does not need to be closed
        explicitly.  See the destructor definition below.

        """
        if self.conn is None:
            conn = psycopg2.connect(dbname=self.setup_details["database"],
                                    host=self.setup_details["host"],
                                    port=self.setup_details["port"],
                                    user=self.setup_details["user"],
                                    password=self.setup_details["password"])
            self.conn = conn
            
        return self.conn

    def __del__(self):
        """No need to explicitly close the connection.  It will be closed when
        the PGConn object is garbage collected by Python runtime.
        """
        print(self.conn)
        self.conn.close()
        self.conn = None
        
def check_schema_exists():
    pass
