#!/usr/bin/env python
import re, sys

def load_phonemap(inpath):
    phonemap = {}
    with open(inpath, 'r') as reader:
        for line in reader:
            phone, ind = line.strip().split('\t')
            phonemap[phone] = ind
    return phonemap

def load_model(inpath):
    gram1_map = {}
    gram2_map = {}
    gram3_map = {}
    backoff2_map = {}
    ngram = None
    skip = True
    
    phonemap = load_phonemap('phonemap.txt')
    with open(inpath, 'r') as reader:
        for line in reader:
            line = line.strip()
            m = re.search('(\d)-grams', line)
            if m :
                skip = False
                ngram = m.group(1)
                continue
            if skip:
                continue
            if ngram == '1':
                subs = line.split('\t') 
                if len(subs) == 3 or len(subs) == 2:
                    phones = re.split('\s+', subs[1])
                    phones = [phonemap[phone] for phone in phones]
                    seq = ' '.join(phones)
                    gram1_map[seq] = float(subs[0])
                else:
                    pass
            elif ngram == '2':
                subs = line.split('\t')
                if len(subs) == 3:
                    log_prob = float(subs[0])
                    log_bfw = float(subs[2])
                    phones = re.split('\s+', subs[1])
                    phones = [phonemap[phone] for phone in phones]
                    seq = ' '.join(phones)
                    
                    gram2_map[seq] = log_prob
                    backoff2_map[seq] = log_bfw

                elif len(subs) == 2:
                    log_prob = float(subs[0])
                    phones = re.split('\s+', subs[1])
                    phones = [phonemap[phone] for phone in phones]
                    seq = ' '.join(phones)
                    gram2_map[seq] = log_prob
                else:
                    pass

            elif ngram == '3':
                try:
                    log_prob, phoneseq = line.split('\t')
                    phones = re.split('\s+', phoneseq)
                    phones = [phonemap[phone] for phone in phones]
                    seq = ' '.join(phones)
                except:
                    pass 
                gram3_map[seq] = float(log_prob)
            else:
                pass
    return gram1_map, gram2_map, gram3_map, backoff2_map

def get_lm_logprob(seq):
    if seq in gram3_map:
        return gram3_map[seq]
    seq = ' '.join(seq.split(' ')[-2 : ])

    if seq in  gram2_map:
        if seq in backoff2_map:
            print gram2_map[seq], backoff2_map[seq]
            return gram2_map[seq] + backoff2_map[seq]
        else:
            return 0
    else:
        return 0


if __name__ == '__main__':
    gram1_map, gram2_map, gram3_map, backoff2_map = load_model('lls_libris_o3.txt')
    print get_lm_logprob('26 26 6')
    print len(gram1_map)
    print len(gram2_map)
    print len(gram3_map)
    print len(backoff2_map)


    '''
    phonemap = load_phonemap('phonemap.txt')
    for phone in phonemap:
        print phone, phonemap[phone] 
    '''
