#!/usr/bin/env python
import numpy as np
from language_model import *
import Queue as Q
from math import log
gram1_map = {}  
gram2_map = {}
gram3_map = {}
backoff2_map = {}

class Path(object):
    def __init__(self, path, prob_ctc, prob_lm, last_index):
        self.path = path
        self.prob_lm = prob_lm #log(10, prob_lm)
        self.prob_ctc = prob_ctc  #log(10, prob_ctc)
        self.last_index = last_index#next expanded label's index in the sorted nodes
    
    def __cmp__(self, other):
        return -1 * cmp(self.prob_ctc, other.prob_ctc)

def get_lm_logprob(seq):
    seq = seq[-3 : ]
    if len(seq) < 2:
        return 0
    tmp = ' '.join(seq)
    if tmp in gram3_map:
        return gram3_map[tmp]
    tmp = ' '.join(seq[-2 : ])
    if tmp in gram2_map:
        if tmp in backoff2_map:
            return gram2_map[tmp] + backoff2_map[tmp]
        else:
            return 0
    else:
        return 0
    
def beam_search_with_lm(prob, beam_width=1000, alpha = 0.01, top_paths=1, merge_repeated=True):
    previous_queue = Q.PriorityQueue()
    previous_queue.put(Path(['<s>'], 0, 0, -1))

    height, width = prob.shape
    #expand frame by frame 
    for i in range(width):
        print 'step: ', i
        probi = prob[:,i]
        topk_index = np.argsort(probi).tolist()[-20 : ] #only expand topk node
        topk_index.reverse() #prob decrease

        select_queue = Q.PriorityQueue()

        while not previous_queue.empty():
            path = previous_queue.get()
            for index in topk_index:
                probK = log(probi[index], 10)
                path_new = path.path[:]
                path_new.append(str(index))
                prob_ctc = path.prob_ctc +  probK
                prob_lm = get_lm_logprob(path_new) +  path.prob_lm
                prob_ctc = prob_ctc + alpha * prob_lm
                select_queue.put(Path(path_new, prob_ctc, prob_lm, index))
        
        cnt = 0
        current_queue = Q.PriorityQueue()
        while not select_queue.empty() and cnt < beam_width:
            path = select_queue.get()
            current_queue.put(path)
            cnt += 1
        previous_queue = current_queue

    result = {}
    while not previous_queue.empty():
        path = previous_queue.get()
        merge_path = joint(path.path) #delete duplicate phone
        if merge_path in result: #merge same paths
            result[merge_path] += path.prob_ctc
        else:
            result[merge_path] = path.prob_ctc
    
    sortresult = sorted(result.items(), key = lambda x : x[1], reverse = True)
    print sortresult[0]    

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
    return ' '.join(templist) 

def build_prob(inpath):
    reader = open(inpath, 'r')
    lines = reader.readlines()
    lines = lines[1:]
    frames = []
    for line in lines:
        nums = [float(num) for num in line.strip().split('\t')]
        frames.append(nums)
    return np.array(frames)


if __name__ == '__main__':
    inpath = '34.prob.txt'
    lm_path = 'lls_libris_o3.txt'
    global gram1_map
    global gram2_map
    global gram3_map
    global backoff2_map

    prob = build_prob(inpath)    
    gram1_map, gram2_map, gram3_map, backoff2_map = load_model(lm_path)
    print prob.shape
    beam_search_with_lm(prob.transpose(), beam_width = 100, alpha = 0.0) 
