include "com.thrift"


/**
 * Region -> Region
 * Operations:
 *  SYNC    := update slaves' data (normal case)
 *  COPY    := copy data to another region
 **/
service Slave {
    i32 sync(1: string sql);
    com.Hits copy(1: string table); // com.Hits ???
}
