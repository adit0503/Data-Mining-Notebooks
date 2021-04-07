import configparser

from tweepy import OAuthHandler, API

class


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('twitterApi.ini')
    tokens = config['twitter.api.tokens']

    auth = OAuthHandler(tokens['ConsumerKey'], tokens['ConsumerSecret'])
    auth.set_access_token(tokens['AccessKey'], tokens['AccessSecret'])

    api = API(auth)

    """
    # get the 20 most recent tweets on your timeline 
    
    public_tweets = api.home_timeline(count=1)
    for tweet in public_tweets:
        print(tweet._json)
    """

    """
    # get user profile and use methods on those instances

    user = api.get_user('twitter')
    print(user.screen_name, user.followers_count)
    for friend in user.friends():
        print(friend.screen_name)
    """


    """
    # get all full extended tweets
    """
    timeline = api.home_timeline(count=1, tweet_mode='extended')
    for tweet in timeline:
        print(tweet)