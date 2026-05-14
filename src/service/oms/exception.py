from uuid import UUID


class BrokerConnectionDoesNotExistException(Exception):

    def __init__(self, deployment_id: UUID):
        super().__init__(
            f"Broker connection for deployment '{deployment_id}' not found"
        )
        self._deployment_id = deployment_id

    @property
    def deployment_id(self):
        return self._deployment_id


class DuplicateOrderException(Exception):
    pass


class OrderNotFoundException(Exception):

    def __init__(self, order_id: UUID):
        super().__init__(f"Order with id '{order_id}' not found")
        self._order_id = order_id

    @property
    def order_id(self) -> UUID:
        return self._order_id
