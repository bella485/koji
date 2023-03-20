class ServerError(Exception):
    ...


class ServerRedirect(ServerError):
    ...


class BadRequest(ServerError):
    ...


class RequestTimeout(ServerError):
    ...
