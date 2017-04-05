#!/usr/bin/env python
import numpy as np
from language_model import *
import Queue as Q
from math import log
import os
gram1_map = {}  
gram2_map = {}
gram3_map = {}
backoff2_map = {}
phonemap = {}
class Path(object):
    def __init__(self, path, prob_ctc, prob_lm, last_index):
        self.path = path
        self.prob_lm = prob_lm #log(10, prob_lm)
        self.prob_ctc = prob_ctc  #log(10, prob_ctc)
        self.prob_lm_last = prob_lm_last
        self.prob_ctc_last = prob_ctc_last
        self.last_index = last_index#next expanded label's index in the sorted nodes
    
    def __cmp__(self, other):
        return -1 * cmp(self.prob_ctc, other.prob_ctc)

def get_lm_logprob(seq, smooth_factor=-6.0):
    if seq[-1] == '39':
        return 0
    if len(seq) > 1 and seq[-1] == seq[-2]:
        return 0
    seq = seq[-3 : ]
    if len(seq) == 1:
        return gram1_map[' '.join(seq)]
    tmp = ' '.join(seq)
    if tmp in gram3_map:
        return gram3_map[tmp]
    tmp = ' '.join(seq[-2 : ])
    if tmp in gram2_map:
        if tmp in backoff2_map:
            return gram2_map[tmp] + backoff2_map[tmp]
        else:
            return smooth_factor
    else:
        return smooth_factor
    
def beam_search_with_lm(prob, beam_width=1000, alpha = 0.0, top_paths=1, merge_repeated=True):
    previous_queue = Q.PriorityQueue(beam_width)
    #previous_queue.put(Path(['40'], 0.0, 0.0, -1))
    previous_queue.put(Path(['40'], 1.0, 1.0, 1.0, 1.0, -1))
    select_queue = Q.PriorityQueue(beam_width)
    for index, prob in enumerate(prob[:,0])
        biggest_path = previous_queue.get()
        path_new = biggest_path.path.append(str(index))
        prob_ctc_last = biggest_path.prob_ctc
        prob_lm_last = biggest_path.prob_lm
        prob_ctc = prob_ctc_last * prob[index, 0]
        prob_lm = pow(10, get_lm_logprob(path_new)) * prob_lm_last
        last_index = 0
        select_queue.put(Path(path_new, prob_ctc, prob_lm, prob_ctc_last, prob_lm_last, last_index))

    height, width = prob.shape
    #expand frame by frame 
    for i in range(1, width):
        #print 'step: ', i
        probi = prob[:,i]
        cur_queue = Q.PriorityQueue(beam_width)
        while not cur_queue.full() and cur_queue.qsize() < previous_queue.qsize() * height:
            biggest_elem = select_queue.get()
            cur_queue.put(biggest_elem)
            index = biggest_elem.last_index
            if index < height - 1:
                path = biggest_elem.path
                last_index = index + 1 
                path_new = path[0:-1].append(str(last_index))
                prob_ctc_last = biggest_elem.prob_ctc_last
                prob_lm_last = biggest_elem.prob_lm_last
                prob_ctc = prob_ctc_last * probi[last_index]
                prob_lm = pow(10, get_lm_logprob(path_new)) * prob_lm_last
                select_queue.put(Path(path_new, prob_ctc, prob_lm, prob_ctc_last, prob_lm_last, last_index))
       previous_queue = cur_queue

    result = {}
    while not previous_queue.empty():
        path = previous_queue.get()
        #print path.path
        merge_path = joint(path.path) #delete duplicate phone
        if merge_path in result: #merge same paths
            result[merge_path] += path.prob_ctc
        else:
            result[merge_path] = path.prob_ctc
    
    sortresult = sorted(result.items(), key = lambda x : x[1], reverse = True)
    return sortresult[0:top_paths]    

def joint(phonelist):
    if phonelist is None or len(phonelist) == 0 :
        return None
    templist = []
    lastphone = ''
    for phone in phonelist:
        if phone == lastphone:
            continue
        else:
            templist.append(phone)
            lastphone = phone
    return ' '.join(templist).replace(' 39','') 

def build_prob(inpath):
    reader = open(inpath, 'r')
    lines = reader.readlines()
    lines = lines[1:]
    frames = []
    for line in lines:
        nums = [float(num) for num in line.strip().split('\t')]
        frames.append(nums)
    return np.array(frames)

def loadphonemap(inpath):
    phonemap = {}
    with open(inpath, 'r') as reader:
        for line in reader:
            phone, ind = line.strip().split('\t')
            phonemap[ind] = phone
    return phonemap

def translate(ind_list):
    print '_'.join(ind_list)


def test():
    inpath = '34.prob.txt'
    prob = build_prob(inpath)    
    result_list = beam_search_with_lm(prob.transpose(), beam_width = 1000, alpha = 0.0, top_paths = 10) 
    for path, prob in result_list:
        phone_ids = path.split(' ')
        phones = [phonemap[phone_id] for phone_id in phone_ids[1 : ]]
        print '%s\t\t\t%f' % (' '.join(phones), prob)

def test_batch(result_file):
    dirname = os.path.dirname(result_file)
    filename = os.path.basename(result_file)
    print dirname, filename
    with open(result_file, 'r') as reader:
        for line in reader:
            print line.strip()
            mfcc_path, prob_path, real_label, predict_label, editdistance_rate, ctc_prob = line.strip().split('\t')
            prob = build_prob(os.path.join(dirname, prob_path.strip()))
            result_list = beam_search_with_lm(prob.transpose(), beam_width = 1000, alpha = 0.00, top_paths = 10) 
            print '####%s' % real_label
            print '----%s\t\t\t%s' % (predict_label, ctc_prob)
            for path, prob in result_list:
                phone_ids = path.split(' ')
                phones = [phonemap[phone_id] for phone_id in phone_ids[1 : ]]
                print '    %s\t\t\t%f' % (' '.join(phones), prob)
            print ''

if __name__ == '__main__':
    lm_path = 'lls_libris_o3.txt'
    phonemap = loadphonemap('phonemap.txt')
    gram1_map, gram2_map, gram3_map, backoff2_map = load_model(lm_path)
    #test()
    test_batch('/home/research/fanjun/temp1/deep_speech/result/good.txt')
