import json
import socket
import configparser

from tweepy import OAuthHandler, StreamListener, Stream


# CONSUMER_KEY = "bGTq5p6hCEtqXvS2J71CxmCSo"
# CONSUMER_SECRET = "NmQqQyeoRYXOI4NX6OViJ5XtPKW3hUVk7W3wuLlXrNJYZBsJyd"
# ACCESS_KEY = "1289149648960020481-30MZ6qJpxyV7xGQsise06JPy5cERS3"
# ACCESS_SECRET = "GJNX6p0jDQHEKsVkGgxtv8EQaE9jqPXv2XOAIScNIgpOD"


class TweetsListener(StreamListener):

    def __init__(self, csocket):
        super().__init__()
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            text = msg['text'].encode('utf-8')
            print(text)
            self.client_socket.send(text)
            return True
        except BaseException as e:
            print("the error is %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    config = configparser.ConfigParser()
    config.read('twitterApi.ini')
    tokens = config['twitter.api.tokens']

    auth = OAuthHandler(tokens['ConsumerKey'], tokens['ConsumerSecret'])
    auth.set_access_token(tokens['AccessKey'], tokens['AccessSecret'])

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['Taylor Swift'])


if __name__ == "__main__":
    s = socket.socket()
    host = "127.0.0.1"
    port = 5555
    s.bind((host, port))

    print("listening on: %s" % str(port))

    s.listen(5)
    c, addr = s.accept()

    print("received from: %s" % str(addr))

    sendData(c)
