from collections import namedtuple


JWTPayload = namedtuple('JWTPayload', ('sub', 'exp'))
