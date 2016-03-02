#! encoding:utf8
# search engine

from collections import defaultdict
from bs4 import BeautifulSoup
from urlparse import urljoin
from sqlite3 import dbapi2 as sqlite
import requests
import pandas as pd
import jieba 
import re
import ipdb
import time

jieba.set_dictionary('dict.txt.big')
url = 'https://news.google.com.tw/'

class Crawler:

    ## crawler for google news, for searching and ranking purpose
    ## only for chinese char is picked.
    ## usage: Crawler('dbname.db')

    def __init__(self,dbname):
        self.con = sqlite.connect(dbname)
    def __del__(self):
        self.con.close()


    def createindextable(self):
        
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table link(fromid iteger,toid integer)')
        self.con.execute('create table wordlocation(wordid,urlid,location)')
        self.con.execute('create table linkword(wordid,linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.con.commit()

    def isindexed(self,url):
    # return true if url is already indexed
        # ipdb.set_trace()
        cur = self.con.execute(
            "select rowid from urllist where url= '%s' " %url
            ).fetchone()
        if cur != None:
            # check it has been crawled
            v = self.con.execute(
                "select * from wordlocation where urlid='%d'" %cur[0]).fetchone()
            if v!= None:            
                return True

        return False

    def addtoIndex(self,url,words):
    # Index a whole page, wordlocation
        if self.isindexed(url): return
        print "Indexing, url: %s" %url
        
        # Get urlid
        urlid = self.getentryid('urllist','url',url)
        # Link each word to this url
        for location,word in enumerate(words):
            wordid = self.getentryid('wordlist','word',word)
            self.con.execute(
                "insert into wordlocation(urlid,wordid,location) values(%d,%d,%d)"
                %(urlid,wordid,location) 
                )
        # db.commit()

    def getentryid(self,table,field,value, create=True):
        # auxilary function to get entry id, if not create it in default
        
        cur = self.con.execute(
                        "select rowid from %s where %s = '%s'" 
                        % (table,field,value)
                        )
        res = cur.fetchone()
        # db.commit()
        # ipdb.set_trace()
        if res == None:
            cur = self.con.execute(
                            "insert into %s (%s) values ('%s')"
                            %(table,field,value)
                            )            
            return cur.lastrowid
        else:             
            return res[0]

    def addlinkref(self,fromurl,tourl,texts):
        # add a link between two pages: linkword/link
        
        fromid = self.getentryid('urllist','url',fromurl)
        toid = self.getentryid('urllist','url',tourl)
        if fromid==toid: return 
        cur = self.con.execute(
                    "insert into link(fromid,toid) values(%d,%d)" % (fromid,toid))
        linkid=cur.lastrowid

        for text in texts:
            # linkword table
            wordid = self.getentryid('wordlist','word',text)
            cur = self.con.execute(
                "insert into linkword(linkid,wordid) values(%d,%d)" %(linkid,wordid)
                                )        
    

    def crawler(self,pages,pagedepth=1):
        # crawl for given pages:[url1,url2,url3...]
        # 
        for depth in range(pagedepth):
            newpages = {}
            for num,page in enumerate(pages):
                try:
                    res=requests.get(page)
                except: 
                    print 'Could not find %s page'%page
                
                soup=BeautifulSoup(res.text)

                # chinese contents
                contents=gettextonly(soup.text)
                words = wordsplit(contents) # split
                
                # indexed wordlist/urllist/wordlocation into db
                self.addtoIndex(page,words)
                print "addtoindex, page:%d"%num
                # url links <a href=''>xx</a>
                links = soup.select('a')

                for i,link in enumerate(links):

                    if 'href' in link.attrs:
                        url = urljoin(page,link['href'])
                        if url.find("'") != -1: continue
                        url = url.split('#')[0]
                        if url[0:4] == 'http':
                            newpages[url]=link.text # link.text->linktitle, url->link
                        linkTexts = wordsplit(gettextonly(link.text))

                        self.addlinkref(page,url,linkTexts)
                        # print "addlinkref, link:%d"%i
                # print 'link %s,newpages %s '%(link,newpages.keys())
                self.con.commit()
            pages = newpages

        # self.con.close()
        # return words,newpages        

    
    def calculatepagerank(self,iterations=20):
        # caluculate page-rangk by iterations(default=20), 
        
        # clear out the current PageRank tables
        self.con.execute('drop table if exists pagerank')
        self.con.execute('create table pagerank(urlid primary key,score)')

        # initialize every url with a PageRank of 1
        self.con.execute('insert into pagerank select rowid, 1.0 from urllist')
        self.con.commit()

        # loop through every urlid,and iterate several times(numbers is iterations)
        for i in range(iterations):
            print 'iterations no: %d'%(i)
            # loop through every urlid
            for (urlid,) in self.con.execute("select rowid from urllist"):
                pr=0.15

                # loop through all pages which link to this one
                for (linker,) in self.con.execute(
                    "select distinct fromid from link where toid=%d"%urlid):
                    # get page rank
                    linkingpr=self.con.execute(
                        'select score from pagerank where urlid=%d' %linker).fetchone()[0]

                    # get the total number links from this linker
                    linkingcounts = self.con.execute(
                        "select count(*) from link where fromid=%d"%linker).fetchone()[0]
                    pr+=0.85*(linkingpr/linkingcounts)

                self.con.execute(
                    "update pagerank set score=%f where urlid=%d"%(pr,urlid))
            self.con.commit()


class searcher:
    ## search 
    def __init__(self,dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def getmatchrows(self,q):
        # q:input query, useage: getmatchrows('中文搜尋') 
        # output: rows,wordid 
        #    --> ([(urlid1,wordlocation1,wordlocation2,...),
        #           (urlid2,wordlocation1,wordlocation2,...),
        #           (),...],[wordid1,wordid2])
        # execute sql: 'select w0.urlid,w0.location,w1.location
        #               from wordlocation w0, wordlocation w1
        #               where w0.urlid=w1.urlid 
        #               and w0.wordid=10
        #               and w1.wordid=17'
        

        # string to build the query
        fieldlist='w0.urlid'
        tablelist=''
        clauselist=''
        wordids=[]
        
        # Split the words by jieba engine
        words=jieba.cut(q,cut_all=False)

    
        for tablenumber,word in enumerate(words):
            ## get wordid
            wordrow = self.con.execute(
            "select rowid from wordlist where word='%s'"%word).fetchone()

            if wordrow!=None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber!=0:
                    tablelist+=','
                    clauselist+=' and '
                    clauselist+='w%d.urlid=w%d.urlid and '%(tablenumber-1,tablenumber)

                fieldlist+=',w%d.location'%tablenumber
                tablelist+='wordlocation w%d'%tablenumber
                # ipdb.set_trace()
                clauselist+='w%d.wordid=%d'%(tablenumber,wordid)

        # create full query from seperate parts

        fullquery = "select %s from %s where %s" %(fieldlist,tablelist,clauselist)
        # ipdb.set_trace()
        try:
            cur = self.con.execute(fullquery)
            rows = [row for row in cur]

            return rows,wordids
        except Exception: print "oops, nothing here!!"

    def getscoredlist(self,rows,wordids):
        # determine the url ranking scores

        # input:
        # -- rows:[(url1,wordloc1,wordloc2,..),(url2,wordloc1,wordloc2),..]
        # -- wordids: [wordid1,wordid2,..]
        # output: 
        # -- totalscores[urlid], a dict of scores for given urlid

        totalscores=dict([(row[0],0) for row in rows])
        weights=[(1,self.locationscore(rows)),
                 (1,self.frequencyscore(rows)),                 
                 (1,self.pagerankscore(rows)),
                 (1,self.linktextscore(rows,wordids))]

        # weights=[(1.0,self.locationscore(rows))]
        # weights=[(0.25,self.locationscore(rows)),
        #          (0.25,self.frequencyscore(rows)),
        #          (0.25,self.inboundlinkscore(rows)),
        #          (0.25,self.pagerankscore(rows))]
        # weights=[(1.0,self.inboundlinkscore(rows))]

        for (weight,scores) in weights:
            for urlid in totalscores:
                totalscores[urlid]+=weight*scores[urlid]

        return totalscores

    def normalizescores(self,scores,smallIsBetter=False):
        ## input:scores --> dictionary of scores: {1:5,2:3,...}
        ## output: norma

        vsmall =0.00001 # avoid division by 0
        if smallIsBetter:
            minscore=min(scores.values())
            return dict([(u,float(minscore)/max(vsmall,l)) for (u,l) in scores.items()])
        else:
            maxscore = max(scores.values())
            if maxscore==0: 
                maxscore =vsmall
            return dict([(u,float(c)/maxscore) for (u,c) in scores.items()])


    def frequencyscore(self,rows):
        # 1.scored by counting word frequency in a page,
        # 
        # input:
        # -- rows:[(url1,wordloc1,wordloc2,..),(url2,wordloc1,wordloc2),..]        
        # output: 
        # -- counts[urlid], word frequency in urlid
        counts=defaultdict(int)

        for row in rows:
            counts[row[0]]+=1
        return self.normalizescores(counts)

    def locationscore(self,rows):
        # 2.scored by catching location of word in a page,
        locations=dict([(row[0],100000) for row in rows])
        for row in rows:
            loc=sum(row[1:])
            if loc<locations[row[0]]:
                locations[row[0]] = loc
        return self.normalizescores(locations,smallIsBetter=True)

    def inboundlinkscore(self,rows):
        # 3. score by in-bound-link
        uniqueurls=set([row[0] for row in rows])
        inboundcount=dict([(u,self.con.execute(\
            'select count(*) from link where toid=%d'%u).fetchone()[0]
        )
        for u in uniqueurls])

        return self.normalizescores(inboundcount)

    def pagerankscore(self,rows):
        # 4. score by page-rank, 
        #    which is proposed by google cofunder, LarryPage
        
        # pageranks = dict([(row[0],self.con.execute(
        #     "select score from pagerank where urlid=%d "%row[0]).fetchone()[0])
        #     for row in rows])

        pageranks = dict([(row[0],self.con.execute(
            "select score from pagerank where urlid=%d"%row[0]).fetchone()[0]) for row in rows])
        # ipdb.set_trace()
        return self.normalizescores(pageranks)
        # maxrank=max(pageranks.values())

    def linktextscore(self,rows,wordids):
        # 5. score by link-text 

        linkscores=dict([(row[0],0) for row in rows])
        # print linkscores
        for wordid in wordids:
            cur=self.con.execute(
                "select link.fromid,link.toid from linkword,link \
                where wordid=%d and linkword.linkid=link.rowid"%wordid
                )
            for (fromid,toid) in cur:                
                if toid in linkscores:
                    # print "toid:%d"%toid                    
                    pr=self.con.execute(
                        "select score from pagerank where urlid=%d"%fromid
                        ).fetchone()[0]
                    linkscores[toid]+=pr
        return self.normalizescores(linkscores)

    def geturlname(self,urlid):        
        return self.con.execute("select url from urllist where rowid=%d"%urlid).fetchone()[0]

    def query(self,q):
        # ipdb.set_trace()
        if self.getmatchrows(q)!= None:
            rows,wordids= self.getmatchrows(q)
            scores=self.getscoredlist(rows,wordids)
            rankedscores = sorted([(score,url) for (url,score) in scores.items()],reverse=True)
            for (score,urlid) in rankedscores[0:10]:
                print "%f\t%s" %(score,self.geturlname(urlid))
        else: pass




## Auxiliary function

def wordsplit(content):
# use jibeba engine to splits chinese articles
    time0 = time.time()
    words = jieba.cut(content,cut_all=False)
    print "elapse splits words: {}".format(time.time()-time0)
    return (word.strip() for word in words if len(word.strip())>1) # iterator


def gettextonly(text):
    pat = re.compile(u'[\u4e00-\u9fa5]+')    
    return ' '.join(re.findall(pat,text)) # 



##################TRIAL PURPOSE#######################

# def newsinfo(soup):
#     # return:newstitle,newslinks(lists)
#     titletext = soup.select('.esc-lead-article-title')    
#     newstitle=[e.text for e in titletext]

#     urls = soup.select('.esc-lead-article-title')
#     newslinks=[url.select('a')[0].attrs['url'] for url in urls]
#     # abstracttexts=soup.select('div.esc-lead-snippet-wrapper')
#     # abstractlists=[e.text for e in abstracttexts]

#     return newstitle,newslinks



# def crawlpage(url):
#     # crawl the page, return content chinese article only,
#     res = requests.get(url)
#     soup = BeautifulSoup(res.text)

#     # article = [e.text for e in soup.select('p') if len(e.text)>2]
#     # content = ' '.join(article)

#     # use re to catch chinese char, [\u4e00-\u9a05]+
#     # pat = re.compile(u'[a-z0-9A-z\u4e00-\u9a05]+')
#     pat = re.compile(u'[\u4e00-\u9fa5]+')
#     content = re.findall(pat,soup.text)
#     content = ' '.join(content)
#     return content





# def isindex(url,soup):
# 	con = sqlite.connect()

# res = requests.get(url)
# soup = BeautifulSoup(res.text)

# titles,urllinks = newsinfo(soup)
# news = {}
# news['title']=titles
# news['urls']=urllinks
# df_news=pd.DataFrame(news)