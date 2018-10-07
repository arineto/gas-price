from ksql import KSQLAPI

client = KSQLAPI('http://localhost:8088')
client.ksql("SET 'auto.offset.reset' = 'earliest';")

# Drop existing streams
client.ksql('DROP STREAM alerts;')
client.ksql('DROP STREAM locations;')
client.ksql('DROP STREAM gas_prices;')

# Creates gas_prices stream
client.ksql('''
    CREATE STREAM gas_prices \
    (id INT, lat DOUBLE, long DOUBLE, price DOUBLE, timestamp BIGINT, joinner INT) \
    WITH (KAFKA_TOPIC='gas_prices', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');
''')

# Creates gas_prices as a table
client.ksql('''
    CREATE TABLE gas_prices \
    (id INT, lat DOUBLE, long DOUBLE, price DOUBLE, timestamp BIGINT, joinner INT) \
    WITH (KAFKA_TOPIC='gas_prices', VALUE_FORMAT='JSON', TIMESTAMP='timestamp', KEY = 'id');
''')

# Creates the location stream
client.ksql('''
    CREATE STREAM locations \
    (id INT, lat DOUBLE, long DOUBLE, timestamp BIGINT, joinner INT) \
    WITH (KAFKA_TOPIC='locations', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');
''')

# Creates the alert stream
client.sql('''
    SELECT L.id, P.id, P.price FROM locations L \
    INNER JOIN gas_prices P ON L.joinner = P.joinner \
    WHERE GEO_DISTANCE(P.lat, P.long, L.lat, L.long, 'KM') < 0.5;
''')
