import logging

class Logger:
    def __init__(self, level=logging.DEBUG, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'):
        name = self.__class__.__name__

        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(format))

        self.logger.addHandler(console_handler)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)