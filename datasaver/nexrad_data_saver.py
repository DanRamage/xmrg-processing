from abc import ABC, abstractmethod


class precipitation_saver(ABC):
    '''
    This is a base class for saving NEXRAD data.
    '''
    @abstractmethod
    def save(self, data):
        pass



