import yaml 

__config = None

def config():
    #global __config
    if not __config:
        with open('./remoteok/extract/config.yaml') as f:
            config = yaml.safe_load(f)

        return config