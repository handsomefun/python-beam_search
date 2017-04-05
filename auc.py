#!/usr/bin/env python
import Queue as Q
class sortObj(object):
    def __init__(self, label, prob):
        this.label = label
        this.prob = prob

    def __cmp__(self, obj):
        return -1 cmp(self.prob, obj.prob)


def auc(inpath):
    queue = Q.PriorityQueue()
    positive = 0
    negtive  = 0
    with open(inpath, 'r') as reader:
        for line in reader:
            label, prob = line.split('\t')
            label = int(label)
            if label == 1:
                positive += 1
            elif label == 0:
                negtive += 1
            else:
                print "illegal value"
            prob = float(prob)
            queue.put(sortObj(label, prob))
    rank = 0
    sum = 0.0
    while not queue.empty():
        rank += 1
        obj = queue.get()
        if obj.label == 1:
            sum += rank
    
    return (sum - positive * (positive + 1.0) / 2) / (positive * negtive)

if __name__ == '__main__':
    value = auc('')
    print value
