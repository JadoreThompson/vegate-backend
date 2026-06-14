import aiokafka
import kafka

from config import KAFKA_BOOTSTRAP_SERVERS


class KafkaProducer(kafka.KafkaProducer):
    def __init__(self, **configs):
        if "bootstrap_servers" not in configs:
            configs["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(**configs)

    @classmethod
    def create(cls, **configs):
        client = cls(**configs)
        return client


class KafkaConsumer(kafka.KafkaConsumer):
    def __init__(self, *topics, **configs):
        if "bootstrap_servers" not in configs:
            configs["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(*topics, **configs)

    @classmethod
    def create(cls, *topics, **configs):
        client = cls(*topics, **configs)
        return client


class AsyncKafkaProducer(aiokafka.AIOKafkaProducer):
    def __init__(self, *args, **kw):
        if "bootstrap_servers" not in kw:
            kw["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(*args, **kw)

    @classmethod
    def create(cls, *args, **kw):
        client = cls(*args, **kw)
        return client


class AsyncKafkaConsumer(aiokafka.AIOKafkaConsumer):
    def __init__(self, *args, **kw):
        if "bootstrap_servers" not in kw:
            kw["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(*args, **kw)

    @classmethod
    def create(cls, *args, **kw):
        client = cls(*args, **kw)
        return client
