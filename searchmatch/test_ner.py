def ner_rewrite(word_ner):
    keys=word_ner.keys()
    word_ner_re={}
    for i in word_ner.keys():
        keep_b = True
        for j in keys:
            if i==j:
                continue
            if i in j:
                keep_b=False
        if keep_b:
            word_ner_re[i]=word_ner[i]

    return word_ner_re


if __name__=="__main__":
    word_ner_re=ner_rewrite({"洗面": 'important', "洗面奶": 'important', "漂亮": 'not_important'})
    print(word_ner_re)


