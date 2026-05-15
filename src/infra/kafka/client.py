import aiokafka
import kafka

from config import KAFKA_BOOTSTRAP_SERVERS


class KafkaProducer(kafka.KafkaProducer):
    def __init__(self, **configs):
        if "bootstrap_servers" not in configs:
            configs["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(**configs)


class KafkaConsumer(kafka.KafkaConsumer):
    def __init__(self, *topics, **configs):
        if "bootstrap_servers" not in configs:
            configs["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(*topics, **configs)


class AsyncKafkaProducer(aiokafka.AIOKafkaProducer):
    def __init__(self, *args, **kw):
        if "bootstrap_servers" not in kw:
            kw["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(*args, **kw)


class AsyncKafkaConsumer(aiokafka.AIOKafkaConsumer):
    def __init__(self, *args, **kw):
        if "bootstrap_servers" not in kw:
            kw["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(*args, **kw)
