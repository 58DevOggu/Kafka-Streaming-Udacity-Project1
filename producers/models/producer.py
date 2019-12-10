"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
SCHEMA_REGISTRY_URL = "http://localhost:8081/"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "schema.registry.url": SCHEMA_REGISTRY_URL          
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=key_schema,
            default_value_schema=value_schema,
        )

    def topic_exists(self,client, topic_name):
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=50)
        return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))


    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        
        client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})
        if (self.topic_exists(client,self.topic_name)):
            logger.info("topic Already Exists - skipping")
            return

        logger.info(f"Creating Kafka topic: {self.topic_name}")

        futures = client.create_topics([
            NewTopic(
                topic=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas
            )
        ])

        for topic, future in futures.items():
            try:
                future.result()
                logger.info("topic created with name")
            except Exception as e:
                logger.fatal("failed to create topic %s: %s", topic, e)

        

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        if self.producer is None:
            return

        logger.info("producer close incomplete - skipping")

    