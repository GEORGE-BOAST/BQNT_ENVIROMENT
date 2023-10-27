from enum import Enum, unique


@unique
class HTTPMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


@unique
class BaseURL(Enum):
    DEV = "http://bci.dev.bloomberg.com"
    ALPHA = "https://api.alpha.bloomberg.com"
    BETA = "https://beta.api.bloomberg.com"
    PROD = "https://api.bloomberg.com"
