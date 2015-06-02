import tweepy
import json
from boto import kinesis
import sys

aws_api_key = None
aws_api_secret = None

streamName = ""
cred_exp = False

# function to push records into the stream
def put_record(stream_name, partition_key, message, cred_exp) :
    if (cred_exp == True) :
        auth = {"aws_access_key_id":aws_api_key, "aws_secret_access_key":aws_api_secret}
        kin = kinesis.connect_to_region("us-east-1", **auth)
    else :   
        kin = kinesis.connect_to_region("us-east-1")

    print '@%s: %s' % (partition_key, message)
    kin.put_record(stream_name, message, partition_key)

# This is the listener, resposible for receiving data
class StdOutListener(tweepy.StreamListener):
    def on_data(self, data):
        # Twitter returns data in JSON format - we need to decode it first
        decoded = json.loads(data)
        put_record(streamName, decoded['user']['screen_name'],data, cred_exp)
        return True

    def on_error(self, status):
        print status

if __name__ == '__main__':
    H = dict(line.strip().split('=') for line in open('twitter.properties'))

    # Authentication details read from twitter.properties. To  obtain these visit dev.twitter.com
    twitter_api_key = H['api_key']
    twitter_api_secret = H['api_secret']
    twitter_access_token_key = H['access_token_key']
    twitter_access_token_secret = H['access_token_secret']

    if (len(sys.argv) < 1) :
        print "Give Stream Name!!"
        sys.exit()
    elif (len(sys.argv) == 2):
        streamName = sys.argv[1]
    elif (len(sys.argv) == 4) :
        streamName = sys.argv[1]
        aws_api_key = sys.argv[2]
        aws_api_secret = sys.argv[3]
        cred_exp = True

    l = StdOutListener()
    auth = tweepy.OAuthHandler(twitter_api_key, twitter_api_secret)
    auth.set_access_token(twitter_access_token_key, twitter_access_token_secret)

    stream = tweepy.Stream(auth, l)
    stream.filter(track=['programming'])
