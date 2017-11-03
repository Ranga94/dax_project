from sner import Ner
from subprocess import Popen
from pathlib import Path


class TaggingUtils:
    def __init__(self, sner_jar_path, sner_class_path):
        self.sner_jar_path = str(Path(sner_jar_path))
        self.sner_class_path = str(Path(sner_class_path))
        self.proc_obj = None
        self.tagger = None

    def get_named_entities(self, text):
        new_text = text.replace('€', '$')
        classified_text = self.tagger.get_entities(new_text)
        return classified_text

    def start(self):
        command = "java -Djava.ext.dirs=./lib -cp {0} edu.stanford.nlp.ie.NERServer " \
                  "-port 9199 -loadClassifier {1}  -tokenizerFactory edu.stanford.nlp.process.WhitespaceTokenizer " \
                  "-tokenizerOptions tokenizeNLs=false".format(self.sner_jar_path, self.sner_class_path)
        self.stop()
        self.proc_obj = Popen(args=command)
        self.tagger = Ner(host='localhost', port=9199)

    def stop(self):
        if self.proc_obj:
            self.proc_obj.kill()
            self.proc_obj.wait()
            self.proc_obj = None

if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('jar')
    parser.add_argument('c')
    args = parser.parse_args()
    t = TaggingUtils(args.jar, args.c)
    text1 = "Google, headquartered in Mountain View, unveiled the new Android phone at the Consumer Electronic Show.  Sundar Pichai said in his keynote that users love their new Android phones."
    text2 = "I went to California with my family"
    t.start()
    print(t.get_named_entities(text1))
    print(t.get_named_entities(text2))
    t.stop()


