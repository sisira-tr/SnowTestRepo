from abc import ABC, abstractmethod


class BaseReport(ABC):

    def __init__(self, session, config):
        self.session = session
        self.config = config

    @abstractmethod
    def build_queries(self):
        pass

    @abstractmethod
    def process(self, raw_data):
        pass

    @abstractmethod
    def generate_output(self, processed_data):
        pass
