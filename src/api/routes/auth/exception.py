class UserAlreadyExistsException(Exception):
    def __init__(self, message: str = "User already exists with username or email"):
        super().__init__(message)


class UserDoesNotExistException(Exception):
    def __init__(self):
        super().__init__("User doesn't exist")
