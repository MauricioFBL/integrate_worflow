
import remoteok.extract.new_offers as ro_ex
import remoteok.transform.clean as ro_tr
import remoteok.load.insert as ro_lo

class RemoteOnlyPipelineExecution:

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass


class RemoteOkPipelineExecution:

    def extract(self):
        ro_ex.NewOffer()

    def transform(self):
        ro_tr.Clean()

    def load(self):
        ro_lo.main()


pipelines = [RemoteOkPipelineExecution(), RemoteOnlyPipelineExecution()]

def main():
    RemoteOkPipelineExecution().transform()
    # for pipeline in pipelines:
    #     pipeline.execute()

if __name__ == '__main__':
    main()
