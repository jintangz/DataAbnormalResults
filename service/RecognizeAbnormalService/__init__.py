from abc import ABCMeta, abstractmethod

class RecognizeAbnormalService(metaclass=ABCMeta):
    @abstractmethod
    def recognize(self, data, *args, **kwargs):
        pass
