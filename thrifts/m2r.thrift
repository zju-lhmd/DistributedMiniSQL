/**
 * Master -> Region
 * Operations:
 *  CREATE  := create table
 *  DROP    := drop table
 *  RECOVER := recover data from somewhere
 *  UPGRADE := become master from slave
 **/
service Worker {
    i32 create(1: string sql);
    i32 drop(1: string table);
    i32 recover(1: string region);
    i32 upgrade(1: string table);
}
