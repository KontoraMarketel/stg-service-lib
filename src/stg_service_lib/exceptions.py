class ProcessingError(Exception):
    """Ошибка обработки сообщения"""

    pass


class ConfigurationError(Exception):
    """Ошибка конфигурации"""

    pass


class ConnectionError(Exception):
    """Ошибка подключения к внешним сервисам"""

    pass
