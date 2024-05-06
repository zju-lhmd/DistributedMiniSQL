include "com.thrift"


/**
 * Client -> Region
 * Operations:
 *  SQL     := execute SQL
 **/
service Executor {
    com.Hits exec(1: string sql) throws (1: com.Error e);
}
