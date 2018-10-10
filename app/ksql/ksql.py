from kafka import KafkaClient
from ksql import KSQLAPI

kafka_client = KafkaClient(hosts=['localhost:9092'])
kafka_client.ensure_topic_exists('gas_prices')
kafka_client.ensure_topic_exists('locations')

client = KSQLAPI('http://localhost:8088')
client.ksql("SET 'auto.offset.reset' = 'earliest';")

# Drop existing streams
client.ksql('DROP STREAM alerts;')
client.ksql('DROP STREAM locations;')
client.ksql('DROP STREAM gas_prices;')

# Creates gas_prices as a stream
client.ksql('''
    CREATE STREAM gas_prices \
    (stationid VARCHAR, lat DOUBLE, long DOUBLE, price DOUBLE, recordtime BIGINT, joinner INT) \
    WITH (KAFKA_TOPIC='gas_prices', VALUE_FORMAT='JSON');
''')

# Creates the location stream
client.ksql('''
    CREATE STREAM locations \
    (userid VARCHAR, lat DOUBLE, long DOUBLE, recordtime BIGINT, joinner INT) \
    WITH (KAFKA_TOPIC='locations', VALUE_FORMAT='JSON');
''')

# Creates the alert stream using the gas_prices stream
client.sql('''
    CREATE STREAM alerts AS \
    SELECT L.userid, L.lat, L.long, L.recordtime, P.stationid, P.price, P.lat, P.long, P.recordtime \
    FROM locations L INNER JOIN gas_prices P WITHIN 1 HOURS ON L.joinner = P.joinner \
    WHERE GEO_DISTANCE(P.lat, P.long, L.lat, L.long, 'KM') < 0.5 \
    AND L.recordtime - P.recordtime <= 3600000;
''')
