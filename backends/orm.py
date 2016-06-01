#!/usr/bin/python
# coding: utf-8


"""
ORM的实现
"""


import time, logging

import db


class Field(object):
    count = 0
    
    def __init__(self, **kw):
        self.__default = kw.get('default')
        self.ddl = kw.get('ddl')
        self.nullable = kw.get('nullable', False)
        self.updatable = kw.get('updatable', True)
        self.insertable = kw.get('insertable', True)
        self._order = Field.count
        Field.count += 1

    @property
    def default(self):
        d = self.__default
        return d() if callable(d) else d


class CharField(Field):
    def __init__(self, **kw):
        if 'ddl' not in kw:
            kw['ddl'] = 'VARCHAR(255)'
        if 'default' not in kw:
            kw['default'] = ''
        super(CharField, self).__init__(**kw)


class IntegerField(Field):
    def __init__(self, **kw):
        if 'ddl' not in kw:
            kw['ddl'] = 'int'
        if 'default' not in kw:
            kw['default'] = 0
        super(IntegerField, self).__init__(**kw)


class TimeField(Field):
    def __init__(self, **kw):
        if 'ddl' not in kw:
            kw['ddl'] = 'double'
        if 'default' not in kw:
            kw['default'] = time.time
        super(TimeField, self).__init__(**kw)


class TextField(Field):
    def __init__(self, **kw):
        if 'ddl' not in kw:
            kw['ddl'] = 'text'
        if 'default' not in kw:
            kw['default'] = ''
        super(TextField, self).__init__(**kw)


def _gen_sql(table_name, mappings):
    sql = ['-- generating SQL for %s:' % table_name]
    sql.append('create table `%s`(' % table_name)
    sql.append(' id int auto_increment not null,')
    for name, field in sorted(mappings.items(), key=lambda x:x[1]._order):
        sql.append(' %s %s not null,' % (name, field.ddl) if not field.nullable else '%s %s,' % (name, field.ddl))
    sql.append(' primary key(`id`)')
    sql.append(') default charset utf8;')
    return '\n'.join(sql)


# Model 相关


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            # 对Model不修改其生成方式
            return type.__new__(cls, name, bases, attrs)
        # 将继承Model的类的 Field字段全部放入 attrs['__mappings__']中
        mappings = {}
        for k, v in attrs.items():
            if isinstance(v, Field):
                mappings[k] = attrs.pop(k)
        attrs['__mappings__'] = mappings
        attrs['__primary_key__'] = 'id'
        if not attrs.get('__table__'):
            attrs['__table__'] = name.lower()
        attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mappings)
        return type.__new__(cls, name, bases, attrs)


class Model(dict):
    __metaclass__ = ModelMetaclass

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)
    
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise KeyError('"Model" has no key "%s"' % key)

    def __setattr__(self, key, value):
        self[key] = value

    @classmethod
    def get(cls, **kw):
        sql = 'select * from `%s` where ' % cls.__table__
        tmp = []
        names, values = zip(*kw.items())
        for name in names:
            tmp.append('%s=?' % name)
        sql += ' and '.join(tmp)
        d = db.select_one(sql, *values)
        return cls(**d)

    @classmethod
    def filter(cls, **kw):
        sql = 'select * from `%s` where ' % cls.__table__
        tmp = []
        names, values = zip(*kw.items())
        for name in names:
            tmp.append('%s=?' % name)
        sql += ' and '.join(tmp)
        d_list = db.select(sql, *values)
        return [cls(**d) for d in d_list]

    @classmethod
    def create(cls, **kw):
        db.insert(cls.__table__, **kw)
        d = cls.get(**kw)
        return cls(**d)

    def insert(self):
        params = {}
        for k, v in self.__mappings__.items():
            if not hasattr(self, k):
                self[k] = v.default
            params[k] = getattr(self, k)
        db.insert(self.__table__, **params)
        d = self.filter(**params)[-1]
        self.id = d.id

    def update(self):
        params = {}
        for k, v in self.__mappings__.items():
            if not hasattr(self, k):
                self[k] = v.default
            params[k] = getattr(self, k)
        names, values = zip(*params.items())
        sql = ['update `%s` set ' % (self.__table__)]
        sql.append(', '.join('`%s`=?' % name for name in names))
        sql.append(' where id=%s' % self.id)
        sql = ''.join(sql)
        db.update(sql, *values)

    def delete(self):
        db.update('delete from `?` where id=?', self.__table__, self.id)

    

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    db.create_engine('root','python','test')
    class Blog(Model):
        __table__ = 'blogs'
        title = CharField()
        create_time = TimeField()
        content = TextField()
        
    b = Blog(title='123',create_time=1.2, content='info')
