class UserAlreadyExistsException(Exception):
    def __init__(self, message: str = "User already exists with username or email"):
        super().__init__(message)


class UserNotFoundExcpetion(Exception):
    def __init__(self):
        super().__init__("User doesn't exist")


class UserNotAuthenticatedException(Exception):
    def __init__(self):
        super().__init__("User not authenticated")


class InvalidCredentialsException(Exception):
    def __init__(self):
        super().__init__("Invalid credentials provided")


class InvalidVerificationCodeException(Exception):
    def __init__(self, code: str):
        super().__init__(f"Invalid verification code '{code}'")


class EmailAlreadyVerifiedException(Exception):
    def __init__(self):
        super().__init__("Email is already verified")
