#!/usr/bin/env python

from random import randrange
from datetime import datetime as dtime

from sqlalchemy import create_engine, Column, Integer, DateTime, String
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError

# create engine
engine_ = "postgresql+psycopg2://{}:{}@{}/{}"
engine_ = engine_.format('tornado', 'tornado', 'localhost', 'tornado')
engine = create_engine(engine_, convert_unicode=True)
db_session = scoped_session(sessionmaker(
    autocommit=False, autoflush=False, bind=engine))

session = db_session()

Base = declarative_base()


class IpRecord(Base):
    __tablename__ = 'iptable'

    user_id = Column(Integer, primary_key=True)
    ip_address = Column(String(15), primary_key=True)
    date = Column(DateTime, default=dtime.now(), nullable=False)


class Reverse(Base):
    __tablename__ = 'reverse'
    ip_address = Column(String(11), primary_key=True)
    user_id = Column(Integer, primary_key=True)


class LastDate(Base):
    __tablename__ = 'last_date'
    value = Column(DateTime, primary_key=True, default=dtime.now())

Base.metadata.create_all(engine)


def generate1():
    lim = [0, 21]
    i = 1
    COUNT = 250000
    print "start generate"
    while i <= COUNT:
        if i % 10000:
            ip = "{}.{}.{}.{}".format(*[randrange(*lim) for _ in xrange(4)])
            session.add(IpRecord(
                user_id="{}".format(randrange(1, 10000)),
                ip_address=ip
            ))
        else:
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                continue
            finally:
                db_session.remove()
            print "save {} fields".format(i)
        i += 1
    print "end generate"


def generate2():
    lim = [0, 80]
    COUNT = 10000000
    STEPS = 10000
    print "start generate"
    for i in range(0, STEPS):
        error = True
        while error is True:
            j = 0
            while j < COUNT // STEPS:
                ip = "{}.{}.{}.{}".format(*[randrange(*lim) for _ in range(4)])
                session.add(IpRecord(
                    user_id="{}".format(randrange(1, 30000)),
                    ip_address=ip
                ))
                j += 1

            try:
                session.commit()
                error = False
            except IntegrityError:
                session.rollback()

            db_session.remove()
    print "end generate"


def generate3():
    records = [{
        'user_id': 1000,
        'ip_address': "192.168.1.1",
    },
    {
        'user_id': 1001,
        'ip_address': "192.168.1.2",
    },
    {
        'user_id': 1002,
        'ip_address': "192.168.2.2",
    }]
    for record in records:
        session.add(IpRecord(**record))
    session.commit()


def generate4():
    records = [{
        'user_id': 1003,
        'ip_address': "192.168.2.3",
    },
    {
        'user_id': 1004,
        'ip_address': "192.168.1.2",
    },
    {
        'user_id': 1004,
        'ip_address': "192.168.3.2",
    }]
    for record in records:
        session.add(IpRecord(**record))
    session.commit()


def select():
    for row in session.query(IpRecord).all():
        print(row)

generate2()
