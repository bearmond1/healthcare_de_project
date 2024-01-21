import snowflake.connector


# db
source_db_connstr = "mysql+mysqlconnector://root:password@host.docker.internal:3306/healthcare"

# SF
account = "account-identificator"
user = "user"
password = "password"
warehouse = "WH"
database = "HEALTHCARE"
schema = "SOURCE_DATA"

def get_sf_conn():
    conn = snowflake.connector.connect(
                    account = account,
                    user = user,
                    password = password,            
                    warehouse=warehouse,
                    database=database,
                    schema = schema
                )
    return conn
