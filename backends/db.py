#!/usr/bin/python
# coding: utf-8


"""
提供对MySQLdb的封装
外部调用方式：
    import db
    db.create_engine(user, passwd, db, **kw)
    db.insert(table, **kw)           --> return cursor.rowcount
    db.update(sql, *args)            --> return cursor.rowcount
    db.select_one(sql, *args)        --> return Dict object or None
    db.select(sql, *args)            --> return [Dictobj, ..., Dictobj] or []

// TODO:
    提供事务操作，用以修复orm中Model类中 insert(self)的BUG
"""


import logging, threading


class _Engine(object):
    def __init__(self, connect):
        # 接受connet函数
        self._connect = connect

    def connect(self):
        return self._connect()


engine = None


def create_engine(user, passwd, db, **kw):
    import MySQLdb
    params = dict(user=user, passwd=passwd, db=db, **kw)
    defaults = {'host': 'localhost', 'port':3306, 'charset': 'utf8'}
    defaults.update(params)
    global engine
    engine = _Engine(lambda: MySQLdb.connect(**defaults))
    logging.info('engine<%s> init success.' % hex(id(engine)))


class _DbCtx(threading.local):
    def __init__(self):
        super(_DbCtx, self).__init__()
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return bool(self.connection)
    
    def init(self):
        self.connection = _LazyConnection()
        self.transactions = 0 

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()


_db_ctx = _DbCtx()


class _LazyConnection(object):
    def __init__(self):
        self.connection = None

    def cursor(self):
        if not self.connection:
            self.connection = engine.connect()
            logging.info('[CONN] [OPEN] connection <%s>' % hex(id(self.connection)))
        return self.connection.cursor()

    def cleanup(self):
        if self.connection:
            logging.info('[CONN] [CLOSE] connection <%s>' % hex(id(self.connection)))
            self.connection.close()
            self.connection = None

    def commit(self):
        if self.connection:
            self.connection.commit()

    def rollback(self):
        if self.connection:
            self.connection.rollback()


class Dict(dict):
    def __init__(self, names, values, **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise KeyError('"Dict" object has no key "%s"' % key)

    def __setattr__(self, key, value):
        self[key] = value 


class With_Connetion(object):
    def __enter__(self):
        self.should_cleanup = False
        global _db_ctx
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True 
        return self 

    def __exit__(self, exctype, excvalue, traceback):
        if self.should_cleanup:
            _db_ctx.cleanup()


def with_connection(func):
    def _wrapper(*args, **kw):
        with With_Connetion():
            return func(*args, **kw)
    return _wrapper


@with_connection
def _select(sql, get_one, *args):
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        global _db_ctx
        cursor = _db_ctx.cursor()
        r = cursor.execute(sql, args)
        names = [i[0] for i in cursor.description]
        if get_one:
            if r:
                return Dict(names, cursor.fetchone())
            return None
        else:
            return [Dict(names, values) for values in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()


def select_one(sql, *args):
    """返回Dict object"""
    return _select(sql, True, *args)


def select(sql, *args):
    """返回由Dict组成的list"""
    return _select(sql, False, *args)


@with_connection
def _insert(table, **kw):
    names, values = zip(*kw.items())
    sql = 'INSERT INTO `%s`(%s) VALUES(%s)' % (
            table,
            ', '.join('`%s`' % name for name in names),
            ', '.join('?' for i in range(len(names)))
        )
    cursor = None
    sql = sql.replace('?','%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, values))
    try:
        global _db_ctx
        cursor = _db_ctx.cursor()
        r = cursor.execute(sql, values)
        _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

def insert(table, **kw):
    return _insert(table, **kw)


@with_connection
def _update(sql, *args):
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    cursor = None
    try:
        global _db_ctx
        cursor = _db_ctx.cursor()
        r = cursor.execute(sql, args)
        _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()


def update(sql, *args):
    """
    >>> u1 = dict(name='zj', score=104, kecheng='yuwen')
    >>> insert('students', **u1)
    1L
    >>> u2 = select_one('select * from students where score=?', 104)
    >>> u2.score
    104L
    >>> u2.kecheng
    u'yuwen'
    >>> update("update students set name=? where score=?", 'zj123123',104)
    1L
    >>> u3 = select_one('select * from students where score=?', 104)
    >>> u3.name
    u'zj123123'
    >>> update('delete from students where name=?', u3.name)
    1L
    """
    return _update(sql, *args)



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    create_engine('root','python','test')
    import doctest
    doctest.testmod()
    
