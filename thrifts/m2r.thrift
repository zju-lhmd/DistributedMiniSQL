/**
 * Master -> Region
 * Operations:
 *  CREATE  := create table
 *  DROP    := drop table
 *  RECOVER := recover data from somewhere
 *  UPGRADE := become master from slave
 **/
service Worker {
    i32 create(1: string sql, 2: list<string> regions);
    i32 drop(1: string table);
    i32 recover(1: string table, 2: string region);
    i32 upgrade(1: string table);
}
