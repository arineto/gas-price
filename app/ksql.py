from ksql import KSQLAPI

client = KSQLAPI('http://localhost:8088')
client.ksql("SET 'auto.offset.reset' = 'earliest';")

# Drop existing streams
client.ksql('DROP STREAM alerts;')
client.ksql('DROP STREAM locations;')
client.ksql('DROP STREAM gas_prices;')

# Creates gas_prices stream
client.ksql(
    "CREATE STREAM gas_prices"
    "(id INT, lat DOUBLE, long DOUBLE, fuel VARCHAR, price DOUBLE, price_timestamp BIGINT)"
    "WITH (KAFKA_TOPIC='gas_prices', VALUE_FORMAT='JSON', TIMESTAMP='price_timestamp');"
)

# Creates the location stream
client.ksql(
    "CREATE STREAM locations"
    "(id INT, lat DOUBLE, long DOUBLE, location_timestamp BIGINT)"
    "WITH (KAFKA_TOPIC='locations', VALUE_FORMAT='JSON', TIMESTAMP='location_timestamp');"
)

# Creates the alert stream
client.sql(
    "SELECT P.id, L.id FROM gas_prices P INNER JOIN locations L WITHIN 4 HOURS ON P.id = L.id;"
)

# Some useful queries
query1 = "SELECT id, TIMESTAMPTOSTRING(price_timestamp, 'yyyy-MM-dd HH:mm:ss') from gas_prices;"