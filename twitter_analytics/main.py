from twitter_mining import get_tweets
import sys
from pathlib import Path
sys.path.insert(0, str(Path('..', 'utils')))
#sys.path.insert(0, '../utils')
import email_tools



'''
languages = ['en', 'en','ja','ar','es','am','hy','bn','bg','my','ckb','zh',
             'da','dv','nl','et','fi','fr','ka','de','el','gu','ht','he','hi',
             'hu','is','id','it','kn','km','ko','lo','lv','lt','ml','mr','ne',
             'no','or','pa','ps','fa','pl','pt','pa','ro','ru','ar','ad','si',
             'sl','sv','tl','ta','te','th','bo','tr','ur','ug','vi']
'''
languages = ['de']

def main(args):
    if not args.translate: #not translate => just English
        get_tweets(args.connection_string,args.database,args.collection,'en',logging=args.logging)

        if args.logging:
            email_tools.send_mail(args.connection_string, args.database,["ulysses@igenieconsulting.com", "hamsa.bharadwaj@igenieconsulting.com",
                                                                         "emil@igenieconsulting.com","hu.kefei@yahoo.co.uk",
                                                                         "angad.virdee.16@ucl.ac.uk","alaka@igenieresourcing.com"])
    else:
        for lang in languages:
            get_tweets(args.connection_string, args.database, args.collection, lang, logging=args.logging)

            if args.logging:
                email_tools.send_mail(args.connection_string, args.database,
                                      ["ulysses@igenieconsulting.com", "hamsa.bharadwaj@igenieconsulting.com",
                                       "emil@igenieconsulting.com", "hu.kefei@yahoo.co.uk",
                                       "angad.virdee.16@ucl.ac.uk", "alaka@igenieresourcing.com"])


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--connection_string", help="MongoDB connection string")
    parser.add_argument("-d", "--database", help="MongoDB database")
    parser.add_argument("-c", "--collection", help="MongoDB collection")
    parser.add_argument("-l", "--logging", action="store_true", help='Logging')
    parser.add_argument("-t", "--translate", action="store_true", help='Other languages')
    args = parser.parse_args()
    main(args)