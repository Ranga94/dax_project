from twitter_mining import get_tweets
import sys
sys.path.insert(0, '../utils')
import email_tools


'''
languages = ['en', 'en','ja','ar','es','am','hy','bn','bg','my','ckb','zh',
             'da','dv','nl','et','fi','fr','ka','de','el','gu','ht','he','hi',
             'hu','is','id','it','kn','km','ko','lo','lv','lt','ml','mr','ne',
             'no','or','pa','ps','fa','pl','pt','pa','ro','ru','ar','ad','si',
             'sl','sv','tl','ta','te','th','bo','tr','ur','ug','vi']
'''
languages = ['de']

def main(argv):
    for lang in languages:
        argv.append(lang)
        get_tweets(argv)

    #email_tools.send_mail(argv[0], argv[1], ["ulysses@igenieconsulting.com"])


if __name__ == "__main__":
    main(sys.argv[1:])