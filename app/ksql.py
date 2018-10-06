from ksql import KSQLAPI

'''
Examples
CREATE STREAM gas_prices
    (id VARCHAR, lat DOUBLE, long DOUBLE, fuel VARCHAR, value DOUBLE)
    WITH (KAFKA_TOPIC='gas_prices', VALUE_FORMAT='JSON');
CREATE STREAM locations
    (id VARCHAR, lat DOUBLE, long DOUBLE)
    WITH (KAFKA_TOPIC='locations', VALUE_FORMAT='JSON');

SELECT id, value FROM gas_prices WHERE fuel = 'gas' and value > 4.50;

SELECT p.id, p.value, l.id from gas_prices P
    INNER JOIN locations L
    on GEO_DISTANCE(p.lat, p.long, l.lat, l.long, 'M') < 500;
'''

client = KSQLAPI('http://localhost:8088')

# Drop existing streams
client.ksql('DROP STREAM alerts')
client.ksql('DROP STREAM locations')
client.ksql('DROP STREAM gas_prices')

# Creates gas_prices stream
client.ksql(
    "CREATE STREAM gas_prices"
    "(id VARCHAR, lat DOUBLE, long DOUBLE, fuel VARCHAR, price DOUBLE, timestamp BIGINT)"
    "WITH (KAFKA_TOPIC='gas_prices', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');"
)

# Creates the location stream
client.ksql(
    "CREATE STREAM locations"
    "(id VARCHAR, lat DOUBLE, long DOUBLE, timestamp BIGINT)"
    "WITH (KAFKA_TOPIC='locations', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');"
)

# Creates the alert stream
client.sql(
    "CREATE STREAM alerts "
)
