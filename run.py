'''
@Descripttion: 捉取整个网络的所有页面链接下来！
@Author: BerryBC
@Version: 0.5.1
@Date: 2020-02-02 11:15:41
@LastEditors  : BerryBC
@LastEditTime : 2020-02-09 14:35:23
'''

from Lib.LMongoDB import claMongoDB
from Lib.LAddPage import claAddPage
from configobj import ConfigObj
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import asyncio
import aiohttp
import threading
import time

strCfgPath = './cfg/dbCfg.ini'
objLinkDB = claMongoDB(strCfgPath, 'mongodb')
objAddPage = claAddPage(objLinkDB)
objParam = ConfigObj(strCfgPath)
intHowManyProxy = int(objParam['param']['HowManyProxy'])
intHowManyPageOneTime = int(objParam['param']['HowManyPageOneTime'])
intLessThenFail = int(objParam['param']['LessThenFail'])
intRequestTimeout = int(objParam['param']['RequestTimeout'])
intSemaphore = int(objParam['param']['Semaphore'])
intDeleteTime = int(objParam['param']['DeleteTime'])
intReusableRepeatTime = int(objParam['param']['ReusableRepeatTime'])
intNewRepeatTime = int(objParam['param']['NewRepeatTime'])
intDeleteRepeatTime = int(objParam['param']['DeleteRepeatTime'])


def funMain():
    print('Program begin : '+time.strftime('%Y-%m-%d %H:%M:%S'))
    funSpyReusablePage()
    funSpyNewPage()


def funSpyReusablePage():
    print(' Entry begin : '+time.strftime('%Y-%m-%d %H:%M:%S'))
    curTarget = objLinkDB.LoadAllData('pagedb-Custom')
    for eleTarget in curTarget:
        strRURL = eleTarget['rURL']
        strTag = eleTarget['tag']
        funSpyWeb(eleTarget['eURL'], strRURL, strTag)
    threading.Timer(60*intReusableRepeatTime, funSpyReusablePage).start()
    print(' Entry end : '+time.strftime('%Y-%m-%d %H:%M:%S'))


def funSpyNewPage():
    print(' New Custom begin : '+time.strftime('%Y-%m-%d %H:%M:%S'))

    curRoot = objLinkDB.LoadAllData('pagedb-Custom')
    for eleRoot in curRoot:
        strRURL = eleRoot['rURL']
        strTag = eleRoot['tag']
        curTarget = objLinkDB.LoadRandomLimit(
            'pagedb-Crawled', {'ct': {'$regex': strRURL, '$options': "i"}, 'ced': False}, intHowManyPageOneTime)
        for eleTarget in curTarget:
            objLinkDB.UpdateOneData(
                'pagedb-Crawled', {'_id': eleTarget['_id']}, {'ced': True})
            funSpyWeb(eleTarget['url'], strRURL, strTag)

    threading.Timer(60*intNewRepeatTime, funSpyNewPage).start()
    print(' New Custom end : '+time.strftime('%Y-%m-%d %H:%M:%S'))


def funSpyWeb(eleWeb, strInRURL, strInTag):
    bolRetry = True
    intTryTime = 0
    arrProxy = objLinkDB.LoadRandomLimit(
        'proxydb', {"fail": {"$lte": intLessThenFail}}, intHowManyProxy)
    arrProxy = list(arrProxy)
    intProxyLen = len(arrProxy)
    while (bolRetry and (intTryTime < intProxyLen)):
        try:
            strProxyToSpy = "http://" + \
                arrProxy[intTryTime]["u"] + \
                ":"+arrProxy[intTryTime]["p"]

            options = Options()

            options.add_argument('--no-sandbox')
            options.add_argument('--disable-gpu')
            options.add_argument('--hide-scrollbars')
            options.add_argument('blink-settings=imagesEnabled=false')
            options.add_argument('--headless')
            options.add_argument('--incognito')
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--disable-software-rasterizer')
            options.add_argument('--disable-extensions')
            options.add_argument(
                '--user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36"')
            options.add_argument('--window-size=1280x1024')
            options.add_argument('--start-maximized')
            options.add_argument('--disable-infobars')

            options.add_argument('--proxy-server='+strProxyToSpy)
            browserChorme = webdriver.Chrome(
                '/usr/bin/chromedriver', chrome_options=options)
            browserChorme.set_page_load_timeout(intRequestTimeout)
            browserChorme.set_script_timeout(intRequestTimeout)
            browserChorme.implicitly_wait(intRequestTimeout*2)
            browserChorme.get(eleWeb)
            strhtml = browserChorme.page_source

            if strhtml != '<html><head></head><body></body></html>':
                time.sleep(int(intRequestTimeout*2))
                strhtml = browserChorme.page_source
                browserChorme.close()
                browserChorme.quit()
                soup = BeautifulSoup(strhtml, 'lxml')
                aFromWeb = soup.select('a')
                for eleA in aFromWeb:
                    strSpyURL = eleA.get('href')
                    if strSpyURL is not None and strSpyURL[:4] != 'http':
                        if strSpyURL[1] == '/':
                            objAddPage.AddToDB('https:'+strSpyURL)
                        elif strSpyURL[0] == '/':
                            objAddPage.AddToDB('https://'+strInRURL+strSpyURL)
                    elif strSpyURL[:4] == 'http':
                        objAddPage.AddToDB(strSpyURL)
                arrWebP = soup.select(strInTag)
                objAddPage.AddPContent(arrWebP)
                bolRetry = False
            else:
                intTryTime += 1
                browserChorme.close()
                browserChorme.quit()
        except Exception as e:
            intTryTime += 1
            browserChorme.close()
            browserChorme.quit()


if __name__ == "__main__":
    funMain()
