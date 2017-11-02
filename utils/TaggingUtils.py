from sner import Ner
from subprocess import Popen
from pathlib import Path


class TaggingUtils:
    def __init__(self, sner_jar_path, sner_class_path):
        self.sner_jar_path = str(Path(sner_jar_path))
        self.sner_class_path = str(Path(sner_class_path))
        self.proc_obj = None
        self.tagger = None

    def start_tagger(self):
        command = "java -Djava.ext.dirs=./lib -cp {0} edu.stanford.nlp.ie.NERServer " \
                  "-port 9199 -loadClassifier {1}  -tokenizerFactory edu.stanford.nlp.process.WhitespaceTokenizer " \
                  "-tokenizerOptions tokenizeNLs=false".format(self.sner_jar_path, self.sner_class_path)
        if not self.proc_obj:
            try:
                self.proc_obj = Popen(args=command)
                print(self.proc_obj.pid)
                self.tagger = Ner(host='localhost', port=9199)
                print("Tagger was started.")
                text = "Google, headquartered in Mountain View, unveiled the new Android phone at the Consumer Electronic Show.  Sundar Pichai said in his keynote that users love their new Android phones."
                classified = self.tagger.get_entities(text)
                print(classified)
                self.proc_obj.terminate()
            except OSError as e:
                print(str(e))
        else:
            print("Tagger has already been started.")

    def stop_tagger(self):
        if self.proc_obj:
            if self.proc_obj.poll():
                self.proc_obj.terminate()
            else:
                print("Tagger is not running...")
        else:
            print("Tagger has not been started.")

    def tag(self, text):
        if self.proc_obj and self.proc_obj.poll() and self.tagger:
            new_text = text.replace('€', '$')
            classified_text = self.tagger.get_entities(new_text)
            return classified_text

if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('jar')
    parser.add_argument('c')
    args = parser.parse_args()
    t = TaggingUtils(args.jar, args.c)
    t.start_tagger()
    #text = "Google, headquartered in Mountain View, unveiled the new Android phone at the Consumer Electronic Show.  Sundar Pichai said in his keynote that users love their new Android phones."
    #classified = t.tag(text)
    #print(classified)
    #t.stop_tagger()


