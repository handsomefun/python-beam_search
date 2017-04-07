#! /usr/bin/python

import sys
from pyspark import SparkContext, SparkConf

def info_mapper(line):
    line = line.strip()
    userid, info = line.split('\x01', 1)
    return (userid, line)


def info_reducer(a, b):
    if not a or not b:
        return ""
    if a.find('questionPage') > -1 and b.find('yfdTopicDisplayed') > -1:
        linea = a
        lineb = b
    elif a.find('yfdTopicDisplayed') > -1 and b.find('questionPage') > -1:
        linea = b
        lineb = a
    else:
        print 'linea : ' + a.encode('utf8')
        print 'lineb : ' + b.encode('utf8')
        return ""
    userid_a, url_a, duration_a, interval_a, gender_a, course_a = linea.split('\x01')
    userid_b, url_b, duration_b, interval_b, gender_b, course_b = lineb.split('\x01')
    
    duration_dist = mergedict(duration_a, duration_b)
    interval_dist = mergedict(interval_a, interval_b)
    gender_dist = mergedict(gender_a, gender_b)
    course_dist = mergedict(course_a, course_b)

    return "reduce: %s\x01%s\x01%s\x01%s" % (duration_dist, interval_dist, gender_dist, course_dist)


def mergedict(stringa, stringb):

    dict_c = {pair.split(':')[0] : int(pair.split(':')[1]) for pair in stringa.split(',')}
    dict_v = {pair.split(':')[0] : int(pair.split(':')[1]) for pair in stringb.split(',')}
    
    res = []
    for key in dict_c:
        value1 = dict_c.get(key,0)
        value2 = dict_v.get(key,0)
        if value1*value2 == 0:
            continue
        res.append("%s:%f" % (key, float(value1)/value2))
    return ','.join(res)


if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]

    conf = SparkConf().setAppName('fanjun').setMaster('yarn-cluster').set('spark.yarn.queue', 'research');
    sc = SparkContext(conf = conf)
    rdd = sc.textFile(input)
    rdd.map(info_mapper).filter(lambda x : x is not None).reduceByKey(info_reducer).filter(lambda x : x is not None).saveAsTextFile(output)


