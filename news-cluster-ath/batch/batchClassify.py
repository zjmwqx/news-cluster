# -*- coding: utf-8 -*-
from utils import mongoUtils
import logging
from logging.config import fileConfig
__author__ = 'Jimin.Zhou'
from datetime import *
import traceback
from model import company,news
import ConfigParser
import io
import pickle
import sys
import pymongo
import mysql.connector
import pandas.io.sql as psql
from scipy.sparse import *
from sklearn.feature_extraction.text import CountVectorizer
from math import *
from numpy import *
from sklearn.cluster import AffinityPropagation

fileConfig('../etc/log.cfg')
LOGGER = logging.getLogger('main')

companyInfoMap = {}
LOGGER.info("init title word idf ser")
wordsDF = pickle.load(file('../etc/titleWordsDf.ser'))
LOGGER.info("init title word idf ser success")

#get all company short names and names
def getCompanyShortName(db2):
    selectSql = "select ins.PARTY_ID, equ.TICKER_SYMBOL, ins.REGISTER_FULL_NAME, sec.SECURITY_NAME_ABBR"
    selectSql += " from INSTITUTION ins, SECURITY sec, EQUITY equ"
    selectSql += " where sec.ASSET_CLASS=3"
    selectSql += " and sec.PARTY_ID = ins.PARTY_ID"
    selectSql += " and equ.EQUITY_TYPE in(1,2)"
    selectSql += " and equ.SECURITY_ID = sec.SECURITY_ID"
    companyInfo = psql.read_sql(selectSql,db2)
    return companyInfo.values

#delete all company names in titles
def replace_name(nn):
    global companyInfoMap
    title = nn[u'newsTitle']
    title = title.replace(u':','')
    title = title.replace(u'：','')
    cpInfo = companyInfoMap[companyInfoMap[:,0]==nn[u'companyID']][0]
    name = cpInfo[2]
    stname = cpInfo[3]
    sttname = cpInfo[3][0:2]
    title = title.replace(name,'')
    title = title.replace(stname,'')
    title = title.replace(sttname,'')
    return title

#bi_gram_tokenizer
def n_gram_tokenizer(s):
    s = s.replace(' ','')
    s = s.replace(u' ','')
    res = []
    for ind in range(len(s)-1):
        res.append(s[ind:ind+2])
    if len(res) == 0:
        res.append(' ')
    return res

#classify a company's titles
def classify(titles):
    global wordsDF
    cv = CountVectorizer(analyzer=n_gram_tokenizer)
    cntContentRes = cv.fit_transform(titles)
    cntContentRes = cntContentRes.astype(int)

    feature = cv.get_feature_names()
    rep = cntContentRes.sum(axis=0).A[0]
    ind = rep > 1
    #没有共用的特征，完全不同
    if sum(array(ind))==0:
        return range(len(titles)), range(len(titles))
    cntContentRes = cntContentRes[:, ind]
    feature = array(feature)[rep>1]
    wdDF_subset = []
    for wd in feature:
        if wordsDF.has_key(wd):
            wdDF_subset.append(wordsDF[wd])
        else:
            wdDF_subset.append(0)
    wdIdf = []
    maxDF = max(wdDF_subset)
    for x in wdDF_subset:
        if x > 0:
            wdIdf.append(log(maxDF/x))
        else:
            wdIdf.append(0)
    wdidfDia = lil_matrix((len(wdIdf),len(wdIdf)))
    wdidfDia.setdiag(wdIdf)
    cntContentRes = cntContentRes.tocsr()
    idfVector = cntContentRes*wdidfDia
    filterMat = idfVector>0
    distMatJ = idfVector*filterMat.T
    JDia = 1/log(distMatJ.diagonal())
    regMat = lil_matrix((len(JDia),len(JDia)))
    regMat.setdiag(JDia)
    distMatJ = distMatJ*regMat

    af = AffinityPropagation(affinity = 'precomputed',preference=2).fit(array(distMatJ.todense()))
    cluster_centers_indices = af.cluster_centers_indices_
    labels = af.labels_
    res = labels
    #preference==2的bug处理
    if not isinstance(res[0], int):
        return zeros(len(titles)), [0]
    return res,cluster_centers_indices

#classify all company's titles
def doClassifyAllTitles(newsCompanyMap):
    for cc in newsCompanyMap:
        nwsList = newsCompanyMap[cc].newsList
        titles = [nws.newsTitle for nws in nwsList]
        #print len(titles)
        if len(titles) > 1:
            res,cluster_centers_indices = classify(titles)
            res = array(res).astype(int)
            ll = len(titles)
            #debug
            # rr = pd.DataFrame({'title':titles, 'category': res, 'center':0})
            #
            # for ind in cluster_centers_indices:
            #     rr['center'].ix[ind] = 1
            # rr = rr.sort('category')
            # rr.to_csv('/root/data/classifyNewsTitle/'+str(cc)+'clsResWithWord2VecPropagation.txt',encoding = 'utf-8',sep = '\t',index = False)

            #print res
            for ind in range(ll):
                nwsList[ind].storyID = nwsList[cluster_centers_indices[res[ind]]].id
            for ind in cluster_centers_indices:
                nwsList[ind].isMain = True
        else:
            nwsList[0].storyID = nwsList[0].id
            nwsList[0].isMain = True

#judge stories if not active
def judgeActiveStory(activeDate, newsCompanyMap):
    global storyMap
    storyMap= {}
    for cc in newsCompanyMap:
        nwsList = newsCompanyMap[cc].newsList
        titles = [nws.newsTitle for nws in nwsList]
        ll = len(titles)
        for ind in range(ll):
            if nwsList[ind].publishTime > activeDate:
                storyMap[nwsList[ind].storyID] = True

#update mongodb: set story, isMain, isActive
def updateMongo(newsCompanyMap, batch):
    global storyMap
    for cc in newsCompanyMap:
        nwsList = newsCompanyMap[cc].newsList
        titles = [nws.newsTitle for nws in nwsList]
        ll = len(titles)
        if batch:
            for ind in range(ll):
                mongoUtils.mongo_update(mongodb['mole-dev'].mole_news_sentiment,
                                        {'_id':nwsList[ind].id},{'$set':{'story_id':nwsList[ind].storyID,
                                                                 'isMain':nwsList[ind].isMain,'isActive':True}})
        else:
            for ind in range(ll):
                if storyMap.get(nwsList[ind].storyID, False) == True:
                    mongoUtils.mongo_update(mongodb['mole-dev'].mole_news_sentiment,
                                            {'_id':nwsList[ind].id},{'$set':{'story_id':nwsList[ind].storyID,
                                                                     'isMain':nwsList[ind].isMain,'isActive':True}})
                else:
                    mongoUtils.mongo_update(mongodb['mole-dev'].mole_news_sentiment,
                                            {'_id':nwsList[ind].id},{'$set':{'story_id':nwsList[ind].storyID,
                                                                     'isMain':nwsList[ind].isMain,'isActive':False}})

#get news from mongo, and return maxpublishTime
def getNewsFromMongo(rs):
    newsCompanyMap = {}
    count = 0
    for nn in rs:
        title = replace_name(nn)
        publishTime = nn['publishDate']
        nws = news.News(nn['_id'], nn['newsID'], nn['companyID'], title, publishTime)
        if not newsCompanyMap.has_key(nws.companyID):
            newsCompanyMap[nws.companyID] = company.Company(nws.companyID)
        newsCompanyMap[nws.companyID].addNews(nws)
        count = count + 1
    LOGGER.info(count)
    return newsCompanyMap

#do job step by step, week by week
def doJobAcc(mongodb,endPublishTime):
    startPublishTime = endPublishTime - timedelta(1)
    LOGGER.info(str(startPublishTime) + " is being process..")
    rsNew = mongoUtils.mongo_find(mongodb['mole-dev'].mole_news_sentiment,
        {'$or':[{'isActive':True},{'publishDate':{'$gte': startPublishTime,'$lt':endPublishTime}}],
         "companyRelateScore":{'$gte':5}} )
    newsCompanyMap = getNewsFromMongo(rsNew)
    doClassifyAllTitles(newsCompanyMap)
    acitveDate = endPublishTime - timedelta(7)
    judgeActiveStory(acitveDate, newsCompanyMap)
    updateMongo(newsCompanyMap, False)

#do bacth week job, the starting week, initializing
def doJobBatchWeek(mongodb):
    y = int(sys.argv[1])
    m = int(sys.argv[2])
    d = int(sys.argv[3])
    initTime = datetime(y,m,d)+timedelta(7)
    rs = mongoUtils.mongo_find(mongodb['mole-dev'].mole_news_sentiment,
                          {"publishDate": {'$gte': datetime(y, m, d),
                                           '$lt': initTime}, "companyRelateScore":{'$gte':5}})
    newsCompanyMap= getNewsFromMongo(rs)
    doClassifyAllTitles(newsCompanyMap)
    updateMongo(newsCompanyMap,True)
    while True:
        if(initTime > datetime.today()):
            break;
        initTime = initTime+ timedelta(1)
        doJobAcc(mongodb,initTime)


def doJobPerDay(mongodb):
    endPublishTime = datetime.today()
    doJobAcc(mongodb,endPublishTime)

if __name__ == '__main__':
    mongodb = None
    config = ConfigParser.ConfigParser()
    config.readfp(io.open("../etc/default.ini"))
    mongo_url = config.get("default", "mongo_url")
    mysql_url = config.get("default", "mysql_url")
    mysql_user = config.get("default", "mysql_user")
    mysql_password = config.get("default", "mysql_password")
    db2 = mysql.connector.connect(host=mysql_url, db='securityMaster2', user=mysql_user,
                              passwd=mysql_password)
    companyInfoMap = getCompanyShortName(db2)
    mongodb = pymongo.MongoClient(mongo_url)
    if len(sys.argv) > 1:
        #do utils week
        try:
            #db store: story id
            doJobBatchWeek(mongodb)
        except Exception, err:
            traceback.print_exc()
        finally:
            try:
                mongodb.close()
            except Exception, err:
                pass
    else:
        try:
            #db store: story id
            doJobPerDay(mongodb)
        except Exception, err:
            traceback.print_exc()
        finally:
            try:
                mongodb.close()
            except Exception, err:
                pass