#!/usr/bin/env python
# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
# Reads Avro data, integration with Confluent Cloud Schema Registry
# Call
# python avro_consumer_ccsr.py -f client.properties -t shoe_promotions
# =============================================================================

from confluent_kafka import DeserializingConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import http.client
import urllib

import ccloud_lib


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    schema_registry_conf = {
        'url': conf['schema.registry.url'],
        'basic.auth.user.info': conf['basic.auth.user.info']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    email_avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        from_dict=ccloud_lib.Email.dict_to_email
    )
    promotion_avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        from_dict=ccloud_lib.Promotion.dict_to_promotion
    )

    # for full list of configurations, see:
    #   https://docs.confluent.io/platform/current/clients/confluent-kafka-python/#deserializingconsumer
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['key.deserializer'] = email_avro_deserializer
    consumer_conf['value.deserializer'] = promotion_avro_deserializer
    consumer = DeserializingConsumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Pushover properties
    pushover_token = ccloud_lib.pushover_configs.get("token")
    pushover_user = ccloud_lib.pushover_configs.get("user")
    token_key = str(pushover_token[0])
    user_key = str(pushover_user[0])
    # Here you can change the numbers. Pushover is not for free.
    max_message = ccloud_lib.pushover_configs.get("max_message")
    max_messages_send = int(max_message[0])
    message_count = 1
    # Process messages
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                email_object = msg.key()
                promotion_object = msg.value()
                print(msg.offset(), msg.key(), msg.value())
                if promotion_object is not None:
                    promotion = promotion_object.promotion_name
                    print("Consumed record with key {} and value {}, Total processed rows {}"
                          .format(email_object.email, promotion, message_count))
                    message_count = message_count + 1
                    if message_count <= max_messages_send:
                        message = 'Promotion: ' + \
                            str(promotion) + ' for Email: ' + \
                            str(email_object.email) + ' ready to execute!'
                        
                        try:
                            conn = http.client.HTTPSConnection(
                                "api.pushover.net:443")
                            conn.request("POST", "/1/messages.json",
                                         urllib.parse.urlencode({
                                             "token": token_key,
                                             "user": user_key,
                                             "message": message,
                                         }), {"Content-type": "application/x-www-form-urlencoded"})
                            response = conn.getresponse()
                            print('Pushover status: %s %s' %
                                  (response.status, response.reason))
                        except Exception as e:
                            print(f"Failed to send notification: {e}")
                        finally:
                            conn.close()  # Make sure to close the connection
        except KeyboardInterrupt:
            break
        except SerializerError as e:
            print(f"Message deserialization failed: {e}")
            # Continue polling after malformed record

    # Leave group and commit final offsets
    consumer.close()
