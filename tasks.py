import logging
from celery import Celery
from psycopg2 import connect

logger = logging.getLogger(__name__)

app = Celery('tasks', broker='amqp://guest:guest@localhost//')

conn = connect(database='tornado', user='tornado', password="tornado",
               host='localhost')
cur = conn.cursor()

@app.task()
def update_reverse_table():
    logger.info('Updating reverse table')

    cur.execute('SELECT "value" FROM last_date')
    last_date = cur.fetchone()[0]
    select_query = 'SELECT DISTINCT ON (user_id, ip_addr) user_id, substring(' \
                   'ip_address from 1 for length(ip_address)-strpos(reverse(ip_address), \'.\')) as ip_addr' \
                   ' FROM iptable'
    if last_date:
        select_query += ' WHERE "date" > %s'
        parameters = (last_date,)
    else:
        parameters = ()
    query = 'INSERT INTO "reverse"(user_id, ip_address) ' + select_query
    cur.execute(query, parameters)

    update_last_date_sql = 'UPDATE last_date SET value=%s'
    logger.info("Last Date SQL: " + update_last_date_sql)
    cur.execute(update_last_date_sql, (last_date,))
    conn.commit()
