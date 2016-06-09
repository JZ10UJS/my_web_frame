#!/usr/bin/python
# coding: utf-8


"""
ORM的实现

Field:
    1. count主要用于 Models字段在数据表的字段顺序
    2. ddl 字段描述 如: varchar(255) 或 int 或 double
    3. default 可以传入函数或其他

ModelMetaclass:
    1. 用于控制Model的子类的生成方式
    2. 将Model的子类中属性字段是Filed type的pop出，放入__mappings__中
        如：
        class User(Model):
            name = CharField()
            age  = IntegerField()
        a = User()
        此时， a具有__mappings__属性 为{'name':<xx.CharField xx>, 'age': <xx.IntegerField xx>}
    3. __sql__ 一个匿名函数，用以生成SQL语句
       默认添加主键 'id int auto_increment primary key'
       可以通过 print a.__sql__() 查看语句
       也可以 db.update(a.__sql__())直接创建表
    4. __table__ 用以设置User的表名，若在定义中未设置，则默认为类名的小写

Model:
    1. 继承于dict, 改写__getattr__和__setattr__
    2. 类方法:
        cls.get(**kw): 返回满足要求的第一条记录，并将其转换为对应 cls 的instance 或 None
        cls.filter(**kw): 返回一组cls instance 或 []
        cls.create(**kw): 调用self.insert()返回 cls instance  // 有BUG
    3. insert(self):
        插入数据库, 由于默认添加主键, 插入之后，执行查找, 然后self.id = id // 有BUG 
    4. update(self): 对数据更新
    5. delte(self): 删除数据库中记录

// TODO:
    1. 提供ForeignKey字段
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
        # attrs['__primary_key__'] = 'id'
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
        """
        有BUG,
        1. 新建conn, 插入, conn.commit(), conn.close()
        2. 新建conn, 查询, conn.commit(), conn.close() filter出最后一个
        若在这两次conn中，有其他conn插入同样的值, 此时filter出来的数据就错误了

        解决方法:
            在db中添加新方法
            新建conn, 插入, 查询, conn.commit(), conn.close()
        """
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
    b = Blog()
    print b.__sql__()
    print b.__table__
