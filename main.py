#!/usr/bin/env python

from random import randrange

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler
from tasks import update_reverse_table
from momoko import Pool

import logging
import logging.handlers
logger = logging.getLogger("tornado.general")


class MainHandler(RequestHandler):
    @property
    def db(self):
        return self.application.db


    @gen.coroutine
    def get(self):
        logger.info('GET')
        self.render('templates/index.html', error=None, result=None)

    @gen.coroutine
    def search_reverse_table(self, user_1, user_2):
        query = 'SELECT ip_address FROM reverse WHERE user_id=%s'
        user_1_ips = yield self.db.execute(query, parameters=(user_1,))
        user_2_ips = yield self.db.execute(query, parameters=(user_2,))
        user_1_ips = set([ip[0] for ip in user_1_ips])
        user_2_ips = set([ip[0] for ip in user_2_ips])
        # subquery = 'SELECT 1 from "reverse" r1 ' \
        #            'JOIN "reverse" r2 ON r1.ip_address = r2.ip_address ' \
        #            'WHERE r1.user_id = %s AND r2.user_id = %s'
        # result = yield self.db.execute("SELECT EXISTS({})".format(subquery),
        #                                parameters=(user_1, user_2))
        # result = result.fetchone()[0]
        result = None
        raise gen.Return((result, user_1_ips, user_2_ips))

    @gen.coroutine
    def search_original_table(self, user_1, user_2, last_date):
        query = 'SELECT ip_address FROM iptable WHERE user_id=%s AND "date" > %s'
        user_1_ips = yield self.db.execute(query, parameters=(user_1, last_date))
        user_2_ips = yield self.db.execute(query, parameters=(user_2, last_date))
        user_1_ips = set([ip[0][0:ip[0].rfind('.')] for ip in user_1_ips])
        user_2_ips = set([ip[0][0:ip[0].rfind('.')] for ip in user_2_ips])

        raise gen.Return((None, user_1_ips, user_2_ips))

    @gen.coroutine
    def post(self):
        user_1 = None
        user_2 = None
        try:
            user_1 = int(self.get_argument('user_1'))
            user_2 = int(self.get_argument('user_2'))
        except ValueError:
            self.redirect('/')

        result = False
        if user_1 and user_2:
            cur = yield self.db.execute('SELECT "value" FROM last_date')
            last_date = cur.fetchone()[0]
            update_reverse_table.delay()
            result, user_1_ips, user_2_ips = yield self.search_reverse_table(user_1, user_2)

            if result is None:
                result = len(user_1_ips & user_2_ips) > 0
            if not result:
                result, user_1_ips_main_table, user_2_ips_main_table = \
                    yield self.search_original_table(user_1, user_2, last_date)
                if result is None:
                    user_1_ips |= user_1_ips_main_table
                    user_2_ips |= user_2_ips_main_table
                    result = len(user_1_ips & user_2_ips) > 0

        self.write(str(result or False))
        self.finish()


if __name__ == '__main__':
    application = Application([
        (r'/', MainHandler),
    ])
    conn_params = {
        'db_name': 'tornado',
        'username': 'tornado',
        'password': 'tornado',
        'host': 'localhost',
        'port': 5432,
    }
    ioloop = IOLoop.instance()
    application.db = Pool(dsn="dbname={db_name} "
                              "user={username} "
                              "password={password} "
                              "host={host} "
                              "port={port}".format(**conn_params),
                          size=1, ioloop=ioloop)
    future = application.db.connect()
    ioloop.add_future(future, lambda f: ioloop.stop())
    ioloop.start()

    application.listen(8888)
    ioloop.start()
