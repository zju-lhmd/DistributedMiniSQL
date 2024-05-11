include "com.thrift"


/**
 * Client -> Master
 * Operations:
 *  QUERY   := search for table location
 *  CREATE  := create table
 *  DROP    := drop table
 **/
service Consultant {
    list<string> query(1: string table);
    list<string> create(1: string table, 2: string sql);
    i32 drop(1: string table);
}
