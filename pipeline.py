
from abc import abstractmethod
import remoteok.extract.new_offers as ro_ex
import remoteok.transform.clean as ro_tr
import remoteok.load.insert as ro_lo


class NewPipeline():

    def __init__(self) -> None:
        self.execute()

    def execute(self):
        self.extract()
        self.transform()
        self.load()

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass


class RemoteOnlyPipelineExecution(NewPipeline):

    def execute(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass


class RemoteOkPipelineExecution():

    def execute(self):
        self.extract()
        self.transform()
        # self.load()

    def extract(self):
        ro_ex.NewOffer()

    def transform(self):
        ro_tr.Clean()

    def load(self):
        ro_lo.main()


pipelines = [RemoteOkPipelineExecution()]


def main():
    for pipeline in pipelines:
        pipeline.execute()


if __name__ == '__main__':
    main()
