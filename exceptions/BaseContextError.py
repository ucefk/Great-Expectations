class BaseContextError(Exception):
    def __init__(self, message):
        self.message = message
