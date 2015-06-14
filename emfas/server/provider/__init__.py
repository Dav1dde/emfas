

class Provider(object):
    def __init__(self, ns):
        pass

    def responsible_for(self, url):
        raise NotImplementedError

    def load(self, url):
        raise NotImplementedError


from emfas.server.provider.youtube import YoutubeProvider


provider = [
    YoutubeProvider
]
