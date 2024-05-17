/**
 * Master -> Region
 * Operations:
 *  CREATE  := create table
 *  DROP    := drop table
 *  RECOVER := recover data from somewhere
 *  UPGRADE := become master from slave
 **/
service m2r {
    oneway void create(1: string table, 2: string sql, 3: list<string> slave_addrs);
    oneway void drop(1: string table);
    oneway void recover(1: string table, 2: list<string> region_addrs);
    oneway void upgrade(1: string table, 2: list<string> slave_addrs);
}