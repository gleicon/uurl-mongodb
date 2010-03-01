#!/usr/bin/env python
# coding: utf-8

import sys,os
import base64
import txmongo
import txredisapi
import cyclone.web
from twisted.python import log
from twisted.internet import defer, reactor
from datetime import datetime

UNIQUE_ID_REF = 'URLSHORT:UNIQ'
OBJECT_ID_REF = 'OBJID:%s'

class MainHandler(cyclone.web.RequestHandler):
    @defer.inlineCallbacks
    def get(self, url=None):
        if not url:
            self.render('index.html')
        elif url == 'main.css':
            self.render('main.css')
        elif url == 'jquery.sparkline.min.js':
            self.render('jquery.sparkline.min.js')
        elif url == 'favicon.ico':
            raise cyclone.web.HTTPError(404)

        elif url.endswith('+'):
            try:
                url = url.rstrip('+')
                u = base64.decodestring(url)
                obj_str={"uuid":int(u)}
                obj = yield self.settings.mongo.urlshotdb.urls.find_one(obj_str)
                dt = datetime.now()
                data = obj['clicks_per_minute']
                hours = data[str(dt.year)][str(dt.month)][str(dt.day)] 
                xlbl= hours.keys() 
                range= hours.values() 
                self.render('stats.html', stats=obj, range=",".join(["%s"%x for x in range]))
            except Exception, e:
                print e
                log.msg(e)
                raise cyclone.web.HTTPError(404)

        else:
            try:
                u = base64.decodestring(url)
                # give redis a try
                obj_id = yield self.settings.redis.get(OBJECT_ID_REF % u)
                if obj_id != None:
                    obj_str={"_id":txmongo.collection.ObjectId(obj_id)}
                else:
                    obj_str={"uuid":int(u)}
                # this part can be moved to mqueuing
                obj = yield self.settings.mongo.urlshotdb.urls.find_one(obj_str)
                ref = self.request.headers.get('HTTP_REFERER')
                res = yield self.settings.mongo.urlshotdb.urls.update(obj_str, {'$inc': {"clicks":1}})
                if ref: 
                    yield self.settings.mongo.urlshotdb.urls.update(obj_str, {'$push': {"referers":ref}})

                res = yield self.settings.mongo.urlshotdb.urls.update(obj_str, {'$push': {"visitors":self.request.remote_ip}})
                dt = datetime.now()
                dk = 'clicks_per_minute.%s.%s.%s.%s' % (dt.year, dt.month, dt.day, dt.hour)
                x = yield self.settings.mongo.urlshotdb.urls.update(obj_str, {"$inc":{dk:1}}, upsert=True, safe=True)

                self.redirect(obj['url'])
            except Exception, e:
                log.msg(e)
                raise cyclone.web.HTTPError(404)
    
    @defer.inlineCallbacks
    def post(self, url):

        u = self.get_argument('u')
        if not u:
            raise cyclone.web.HTTPError(500)
        if not u.startswith('http://') and not u.startswith('https://'):
            u='http://'+u
        c = yield self.settings.redis.get(str(u)) # handler for duplicated URLs
        if c != None:
            # url already exists, redirect to stats page
            self.redirect('/%s+' % c)
            return
        uuid = yield self.settings.redis.incr(UNIQUE_ID_REF)
        e_url = base64.encodestring(str(uuid))
        e_url = e_url.rstrip('\n')
#        res = yield self.settings.redis.set(e_url, u) # can be used to bypass stats in a nginx module
        res = yield self.settings.redis.set(str(u), e_url) # handler for duplicated URLs
        # this part can be moved to mqueuing
        dt = datetime.now().ctime()
        objid = yield self.settings.mongo.urlshotdb.urls.insert({"uuid": uuid,\
                "url":u, "e_url": e_url, 'clicks':0, 'referers':[], 'visitors':[], 'clicks_per_minute':{}, 'date':dt}, safe=True)
        res = yield self.settings.redis.set(OBJECT_ID_REF % uuid, str(objid))
        e_url_plus = "%s+" % e_url
        self.render('resp.html', e_url=e_url, e_url_plus=e_url_plus)        

class Application(cyclone.web.Application):
    def __init__(self):
        handlers = [
            (r"/(.*)", MainHandler),
        ]
        settings = dict(
            cookie_secret="32oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
            mongo=txmongo.lazyMongoConnectionPool(),
            redis=txredisapi.lazyRedisConnectionPool(),
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
        )
        cyclone.web.Application.__init__(self, handlers, **settings)

def main(port):
    reactor.listenTCP(port, Application())
    reactor.run()

if __name__ == '__main__':
    log.startLogging(sys.stdout)
    main(8888)
