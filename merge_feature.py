#! /usr/bin/python
#-*- coding:utf-8 -*-

import sys
from pyspark import SparkContext, SparkConf

def frog_mapper(line):
    try:
        line = line.strip()
        index = line.find('UDID=')
        if index > -1:
            index2 = line.find('\t',index+1)
            if index2 > index:
                udid = line[index+5:index2]
                return (udid, line)
    except:
        print  sys.exc_info(), 'error line:' + str(sys.exc_info()[2].tb_lineno)

def raw_mapper(line):
    try:
        line = line.strip()
        fields = line.split('\t')
        if len(fields) > 9:
            userid = fields[9]
            if userid != None:
                return (userid, line)
    except:
        print  sys.exc_info(), 'error line:' + str(sys.exc_info()[2].tb_lineno)

def docvec_mapper(line):
    try:
        line = line.decode('utf-8').strip()
        userid, docvec = line.split("\t")
        return (userid, docvec)
    except:
        print line.encode('utf-8')
        print  sys.exc_info(), 'error line:' + str(sys.exc_info()[2].tb_lineno)

def lecture_mapper(line):
    features = line.strip().split('\x01')
    if len(features) != 23:
        return None
    recid = features[0]
    enrollment = features[3]
    goodrate = features[4]
    url = features[12]
    price = features[13]
    courseid = features[14]
    grade = features[15]
    week = features[16]
    hour = features[17]
    duration = features[18]
    province_teacher = features[19]
    city_teacher = features[20]
    gender = features[21]
    
    if url == '/event/yfdTopicDisplayed':
        return (recid, enrollment+'\x01'+goodrate+'\x01'+price+'\x01'+courseid +'\x01' + grade + '\x01' + week 
                    + '\x01' + hour + '\x01' + duration + '\x01' + province_teacher + '\x01' + city_teacher + '\x01' + gender) 

def new_mapper(line):
    try:
        line = line.strip().split('\x01')
        if len(line) >8:
            userid = line[0]
            ##venfor, province, city, operator, 
            info = line[1] + '\x01' + line[4] + '\x01' + line[5] + '\x01' + line[6]
            competitors_install = line[7]
            competitors_last = line[8]
            ## 
            info = info + '\x01' + competitors_install + '\x01' + competitors_last 
            ## + '\x01' + line[13] + '\x01' + line[14] + '\x01' + line[15] + '\x01' + line[16]
            if userid != None:
                return (userid, info)
    except:
        print  sys.exc_info(), 'error line:' + str(sys.exc_info()[2].tb_lineno)

def action_mapper(line):
    try:
        line = line.strip()
        info = line.split('\x01')
        if len(info) == 7 or len(info) == 11:
            userid = info[0]
            if userid != None:
                return (info[0], line)
    except:
        print  sys.exc_info(), 'error line:' + str(sys.exc_info()[2].tb_lineno)

def final_mapper(line):
    result = []
    for k in line[1]:
        if k is None:
            result.append('None')
        elif isinstance(k, tuple):
            for item in k:
                if item is None:
                    result.append('None')
                else:
                    result.append(item)
        else:
            result.append(k)
    if result is not None:
        return '\x05'.join(result)
def final_mapper1(line):
    if isinstance(line[1], tuple):
        frog = line[1][0]
        vec = line[1][1]
        fields = frog.split('\t')
        if len(fields) > 9:
            userid =  fields[9]
            return (userid, frog + '\x05' + vec)

def final_mapper2(line):
    result = []
    for k in line[1]:
        if k is None:
            result.append('None')
        elif isinstance(k, tuple):
            for item in k:
                if item is None:
                    result.append('None')
                elif isinstance(item, tuple):
                    for it in item:
                        if it is None:
                            result.append('None')
                        else:
                            result.append(it)
                else:
                    result.append(item)
        else:
            result.append(k)
    if result is not None:
        return '\x05'.join(result)

def click_mapper(line):
    try:
        line = line.strip().split('\t',1)
        return (line[0],line[1])
    except:
        print  sys.exc_info(), 'error line:' + str(sys.exc_info()[2].tb_lineno)

if __name__ == '__main__':

    conf = SparkConf().setAppName('new_frog_data').setMaster('yarn-cluster').set("spark.yarn.queue","research").set("spark.files.overwrite","true");
    sc = SparkContext(conf=conf)
    for i, arg in enumerate(sys.argv):
        print "argv[%d]\t%s\t" % (i, arg)
    raw_data = sc.textFile(sys.argv[1]).map(frog_mapper).filter(lambda x : x is not None)
    print "frog count : %d" % raw_data.count()
    keypoint_data = sc.textFile(sys.argv[4]).map(docvec_mapper).filter(lambda x: x is not None)
    print "user vector : %d" % keypoint_data.count()
    frog_vec = raw_data.join(keypoint_data).map(final_mapper1).filter(lambda x: x is not None)
    #userstatic 
    user_data = sc.textFile(sys.argv[2]).map(new_mapper).filter(lambda x : x is not None)
    print "user static into count : %d" %  user_data.count()
    #actionfeature
    action_data = sc.textFile(sys.argv[3]).map(action_mapper).filter(lambda x : x is not None)
    
    output = frog_vec.leftOuterJoin(user_data).leftOuterJoin(action_data).map(final_mapper2).filter(lambda x: x is not None)
    print "output data count : %d" % output.count()
    output.saveAsTextFile(sys.argv[5])
