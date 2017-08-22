from twitter_mining import get_tweets
import sys

def main(argv):
    get_tweets(argv)
    #send email


if __name__ == "__main__":
    main(sys.argv[1:])