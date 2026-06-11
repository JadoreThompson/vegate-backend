import aiokafka
import kafka

from config import KAFKA_BOOTSTRAP_SERVERS
from .ext.retry import KafkaRetryClient


class KafkaProducer(kafka.KafkaProducer):
    def __init__(self, **configs):
        if "bootstrap_servers" not in configs:
            configs["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(**configs)

    @classmethod
    def create(cls, retry: bool = True, **configs):
        client = cls(**configs)
        return KafkaRetryClient(client) if retry else client


class KafkaConsumer(kafka.KafkaConsumer):
    def __init__(self, *topics, **configs):
        if "bootstrap_servers" not in configs:
            configs["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(*topics, **configs)

    @classmethod
    def create(cls, *topics, retry: bool = True, **configs):
        client = cls(*topics, **configs)
        return KafkaRetryClient(client) if retry else client


class AsyncKafkaProducer(aiokafka.AIOKafkaProducer):
    def __init__(self, *args, **kw):
        if "bootstrap_servers" not in kw:
            kw["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(*args, **kw)

    @classmethod
    def create(cls, retry: bool = True, **kw):
        client = cls(**kw)
        return KafkaRetryClient(client) if retry else client


class AsyncKafkaConsumer(aiokafka.AIOKafkaConsumer):
    def __init__(self, *args, **kw):
        if "bootstrap_servers" not in kw:
            kw["bootstrap_servers"] = KAFKA_BOOTSTRAP_SERVERS
        super().__init__(*args, **kw)

    @classmethod
    def create(cls, retry: bool = True, **kw):
        client = cls(**kw)
        return KafkaRetryClient(client) if retry else client
