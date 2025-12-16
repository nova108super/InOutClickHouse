import clickhouse_connect

ch = clickhouse_connect.get_client(
host ='localhost',
port = 8123,
username='click',
password='click',
)

df = ch.query_df(
query="SELECT * FROM users",
)

print(df)
