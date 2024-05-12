#
# Autogenerated by Thrift Compiler (0.20.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
from thrift.TRecursive import fix_spec

import sys
import logging
from .ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
all_structs = []


class Iface(object):
    """
    Client -> Master
    Operations:
     QUERY   := search for table location
     CREATE  := create table
     DROP    := drop table


    """
    def query(self, client_addr, table):
        """
        Parameters:
         - client_addr
         - table

        """
        pass

    def create(self, client_addr, table, sql):
        """
        Parameters:
         - client_addr
         - table
         - sql

        """
        pass

    def drop(self, client_addr, table):
        """
        Parameters:
         - client_addr
         - table

        """
        pass


class Client(Iface):
    """
    Client -> Master
    Operations:
     QUERY   := search for table location
     CREATE  := create table
     DROP    := drop table


    """
    def __init__(self, iprot, oprot=None):
        self._iprot = self._oprot = iprot
        if oprot is not None:
            self._oprot = oprot
        self._seqid = 0

    def query(self, client_addr, table):
        """
        Parameters:
         - client_addr
         - table

        """
        self.send_query(client_addr, table)

    def send_query(self, client_addr, table):
        self._oprot.writeMessageBegin('query', TMessageType.ONEWAY, self._seqid)
        args = query_args()
        args.client_addr = client_addr
        args.table = table
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def create(self, client_addr, table, sql):
        """
        Parameters:
         - client_addr
         - table
         - sql

        """
        self.send_create(client_addr, table, sql)

    def send_create(self, client_addr, table, sql):
        self._oprot.writeMessageBegin('create', TMessageType.ONEWAY, self._seqid)
        args = create_args()
        args.client_addr = client_addr
        args.table = table
        args.sql = sql
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def drop(self, client_addr, table):
        """
        Parameters:
         - client_addr
         - table

        """
        self.send_drop(client_addr, table)

    def send_drop(self, client_addr, table):
        self._oprot.writeMessageBegin('drop', TMessageType.ONEWAY, self._seqid)
        args = drop_args()
        args.client_addr = client_addr
        args.table = table
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()


class Processor(Iface, TProcessor):
    def __init__(self, handler):
        self._handler = handler
        self._processMap = {}
        self._processMap["query"] = Processor.process_query
        self._processMap["create"] = Processor.process_create
        self._processMap["drop"] = Processor.process_drop
        self._on_message_begin = None

    def on_message_begin(self, func):
        self._on_message_begin = func

    def process(self, iprot, oprot):
        (name, type, seqid) = iprot.readMessageBegin()
        if self._on_message_begin:
            self._on_message_begin(name, type, seqid)
        if name not in self._processMap:
            iprot.skip(TType.STRUCT)
            iprot.readMessageEnd()
            x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
            oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
            x.write(oprot)
            oprot.writeMessageEnd()
            oprot.trans.flush()
            return
        else:
            self._processMap[name](self, seqid, iprot, oprot)
        return True

    def process_query(self, seqid, iprot, oprot):
        args = query_args()
        args.read(iprot)
        iprot.readMessageEnd()
        try:
            self._handler.query(args.client_addr, args.table)
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

    def process_create(self, seqid, iprot, oprot):
        args = create_args()
        args.read(iprot)
        iprot.readMessageEnd()
        try:
            self._handler.create(args.client_addr, args.table, args.sql)
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

    def process_drop(self, seqid, iprot, oprot):
        args = drop_args()
        args.read(iprot)
        iprot.readMessageEnd()
        try:
            self._handler.drop(args.client_addr, args.table)
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

# HELPER FUNCTIONS AND STRUCTURES


class query_args(object):
    """
    Attributes:
     - client_addr
     - table

    """


    def __init__(self, client_addr=None, table=None,):
        self.client_addr = client_addr
        self.table = table

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.client_addr = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.table = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('query_args')
        if self.client_addr is not None:
            oprot.writeFieldBegin('client_addr', TType.STRING, 1)
            oprot.writeString(self.client_addr.encode('utf-8') if sys.version_info[0] == 2 else self.client_addr)
            oprot.writeFieldEnd()
        if self.table is not None:
            oprot.writeFieldBegin('table', TType.STRING, 2)
            oprot.writeString(self.table.encode('utf-8') if sys.version_info[0] == 2 else self.table)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(query_args)
query_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'client_addr', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'table', 'UTF8', None, ),  # 2
)


class create_args(object):
    """
    Attributes:
     - client_addr
     - table
     - sql

    """


    def __init__(self, client_addr=None, table=None, sql=None,):
        self.client_addr = client_addr
        self.table = table
        self.sql = sql

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.client_addr = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.table = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.sql = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('create_args')
        if self.client_addr is not None:
            oprot.writeFieldBegin('client_addr', TType.STRING, 1)
            oprot.writeString(self.client_addr.encode('utf-8') if sys.version_info[0] == 2 else self.client_addr)
            oprot.writeFieldEnd()
        if self.table is not None:
            oprot.writeFieldBegin('table', TType.STRING, 2)
            oprot.writeString(self.table.encode('utf-8') if sys.version_info[0] == 2 else self.table)
            oprot.writeFieldEnd()
        if self.sql is not None:
            oprot.writeFieldBegin('sql', TType.STRING, 3)
            oprot.writeString(self.sql.encode('utf-8') if sys.version_info[0] == 2 else self.sql)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(create_args)
create_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'client_addr', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'table', 'UTF8', None, ),  # 2
    (3, TType.STRING, 'sql', 'UTF8', None, ),  # 3
)


class drop_args(object):
    """
    Attributes:
     - client_addr
     - table

    """


    def __init__(self, client_addr=None, table=None,):
        self.client_addr = client_addr
        self.table = table

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.client_addr = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.table = iprot.readString().decode('utf-8', errors='replace') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('drop_args')
        if self.client_addr is not None:
            oprot.writeFieldBegin('client_addr', TType.STRING, 1)
            oprot.writeString(self.client_addr.encode('utf-8') if sys.version_info[0] == 2 else self.client_addr)
            oprot.writeFieldEnd()
        if self.table is not None:
            oprot.writeFieldBegin('table', TType.STRING, 2)
            oprot.writeString(self.table.encode('utf-8') if sys.version_info[0] == 2 else self.table)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(drop_args)
drop_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'client_addr', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'table', 'UTF8', None, ),  # 2
)
fix_spec(all_structs)
del all_structs
