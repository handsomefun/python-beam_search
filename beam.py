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
    previous_queue.put(Path(['40'], 1.0, 1.0, -1))
    height, width = prob.shape
    #expand frame by frame 
    for i in range(width):
        #print 'step: ', i
        probi = prob[:,i]
        topk_index = np.argsort(probi)[-1 * 20 : ].tolist() #only expand topk node
        topk_index.reverse() #prob decrease

        select_queue = Q.PriorityQueue()

        while not previous_queue.empty():
            path = previous_queue.get()
            for index in topk_index:
                #probK = log(probi[index],10)
                probK = probi[index]
                path_new = path.path[:]
                path_new.append(str(index))
                #prob_ctc = path.prob_ctc +  probK
                prob_ctc = path.prob_ctc * probK
                prob_lm = pow(10, get_lm_logprob(path_new)) * path.prob_lm
                #print 'prob_lm = %f, prob_ctc = %f' % (log(prob_lm, 10), log(prob_ctc, 10))
                prob_ctc = prob_ctc + alpha * prob_lm
                #print path_new, pow(10, prob_ctc)
                select_queue.put(Path(path_new, prob_ctc, prob_lm, index))
            #print '+++++++++++++' 
        cnt = 0
        current_queue = Q.PriorityQueue(beam_width)
        while not select_queue.empty() and cnt < beam_width:
            path = select_queue.get()
            current_queue.put(path)
            cnt += 1
        previous_queue = current_queue

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
