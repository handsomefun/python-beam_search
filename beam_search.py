#!/usr/bin/env python
import numpy as np

def beam_search(prob, beam_width=1000, top_paths=1, merge_repeated=True):
    paths_prob = {'': 1.0}
    height, width = prob.shape
    
    for i in range(width):
        print 'step: ', i
        probi = prob[:,i]
        topk_index = np.argsort(probi)[-1 * 20 : ].tolist() #only expand topk node
        topk_index.reverse()
        paths_prob_expand = {}
        for path in paths_prob:
            prob_path = paths_prob[path]
            for k in topk_index:
                prob_k = probi[k]
                path_new = path + ',%d' % k 
                prob_new = prob_k * prob_path
                if path_new in paths_prob_expand:
                    paths_prob_expand[path_new] += prob_new 
                else:
                    paths_prob_expand[path_new] = prob_new
                
        sorted_pairs = sorted(paths_prob_expand.items(), key = lambda x: x[1], reverse = True)
        if len(sorted_pairs) > beam_width:
            sorted_pairs = sorted_pairs[ : beam_width]
        paths_prob = dict(sorted_pairs) 
    
    result = {}
    for path in paths_prob:
        merge_path = joint(path) #delete duplicate phone
        if merge_path in result: #merge same paths
            result[merge_path] += paths_prob[path]
        else:
            result[merge_path] = paths_prob[path]
    
    sortresult = sorted(result.items(), key = lambda x : x[1], reverse = True)
    print sortresult[0]    

def joint(line):
    if line and len(line) == 0:
        return None
    if line.startswith(','):
        line = line[1:]
    phonelist = []
    templist = line.split(',')
    lastphone = ''
    for phone in templist:
        if phone == lastphone:
            continue
        else:
            phonelist.append(phone)
            lastphone = phone
    return ','.join(phonelist) 

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
    prob = build_prob(inpath)    
    print prob.shape
    beam_search(prob.transpose()) 
