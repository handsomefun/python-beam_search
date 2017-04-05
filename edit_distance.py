#!/usr/bin/env python
import re
import numpy as np
multikeymap = {'AA': '1', 'AW': '32', 'DH': '0', 'Y': '30', 'HH': '19', 'CH': '24', 'JH': '36', 'ZH': '38', 'D': '29',
        'NG': '11', 'TH': '2', 'IY': '8', 'B': '7', 'AE': '1', 'EH': '20', 'G': '9', 'F': '14', 'AH': '1', 'K': '15',
        'M': '6', 'L': '16', 'AO': '4', 'N': '11', 'IH': '8', 'S': '2', 'R': '5', 'EY': '20', 'T': '3', 'W': '17', 'V':
        '22', 'AY': '27', 'Z': '25', 'ER': '13', 'P': '31', 'UW': '28', 'SH': '35', 'UH': '28', 'OY': '37', 'OW': '23'}

def load(inpath):
    global multikeymap
    with open(inpath) as reader:
        for line in reader:
            key, value = re.split('\t', line.strip())
            multikeymap[key] = value

def distance(stra, strb):
    list_a = re.split('\s+', stra.strip())
    list_b = re.split('\s+', strb.strip())
    len_a = len(list_a) + 1
    len_b = len(list_b) + 1

    matrix = np.zeros((len_a, len_b)) 
    for i in range(len_a):
        matrix[i,0] = i

    for j in range(len_b):
        matrix[0, j] = j

    for i in range(1, len_a):
        for j in range(1, len_b):
            if list_a[i-1] == list_b[j-1]:
                cost = 0
            else:
                cost = 1
            matrix[i, j] = min(matrix[(i - 1) , j] + 1 ,
                                        matrix[i, (j - 1)] + 1,
                                        matrix[(i - 1), (j - 1)] + cost)
    return matrix[len_a - 1, len_b - 1]

def distance_extends(stra, strb):
    list_a = re.split('\s+', stra.strip())
    list_b = re.split('\s+', strb.strip())
    len_a = len(list_a) + 1
    len_b = len(list_b) + 1

    matrix = np.zeros((len_a, len_b)) 
    for i in range(len_a):
        matrix[i,0] = i

    for j in range(len_b):
        matrix[0, j] = j

    for i in range(1, len_a):
        for j in range(1, len_b):
            if maybe_equal(list_a[i-1], list_b[j-1]):
                cost = 0
            else:
                cost = 1
            matrix[i, j] = min(matrix[(i - 1) , j] + 1,
                                        matrix[i, (j - 1)] + 1,
                                        matrix[(i - 1), (j - 1)] + cost)
    print matrix
    return matrix[len_a - 1, len_b - 1]

def maybe_equal(ch_a, ch_b):
    if multikeymap[ch_a] == multikeymap[ch_b]:
        return True
    else:
        return False

if __name__ == '__main__':
    stra = 'B L UW'
    strb = 'B UW'
    load('multikey.txt')
    print multikeymap
    print distance(stra, strb)
