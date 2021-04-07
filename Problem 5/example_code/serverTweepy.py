import configparser
import json
import socket

from tweepy import OAuthHandler, Stream, StreamListener


class Twitter:
    def __init__(self):        
        _config = configparser.ConfigParser()
        _config.read('../twitterApi.ini')
        _tokens = _config['twitter.api.tokens']

        self.auth = OAuthHandler(_tokens['ConsumerKey'], _tokens['ConsumerSecret'])
        self.auth.set_access_token(_tokens['AccessKey'], _tokens['AccessSecret'])

    def getAPI(self):
        return API(self.auth)

    def getStream(self, streamListener):
        return Stream(auth=self.auth, listener=streamListener)


class MyListener(StreamListener):
    def __init__(self, client):
        super().__init__()
        self.client = client

    def on_data(self, raw_data):
        try:
            data = json.loads(raw_data)
            # self.client.send(data['text'].encode('utf-8'))
            for hashtag in data['entities']['hashtags']:
                print(hashtag['text'])
                # self.client.send(hashtag['text'].encode('utf-8'))
            self.client.send(data['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            return True

    def on_error(self, status):
        print(status)
        return False


if __name__ == "__main__":
    
    HOST = 'localhost'
    PORT = 12345

    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen(10)
        client, addr = server.accept()

        # sendData(client)
        twitter = Twitter()
        stream = twitter.getStream(MyListener(client))
        stream.filter(track=['#'])