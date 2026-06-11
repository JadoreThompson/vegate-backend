from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError

from module.exception.retry import Retry

db_retry = Retry(exceptions=[OperationalError])


class RetrySession:

    def __init__(self, session: Session):
        self.session = session

    def __getattribute__(self, name):
        if name == "session":
            return object.__getattribute__(self, "session")
        
        attr = getattr(self.session, name)

        if not callable(attr):
            return attr

        return db_retry(attr)
