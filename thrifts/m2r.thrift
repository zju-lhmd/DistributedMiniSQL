/**
 * Master -> Region
 * Operations:
 *  CREATE  := create table
 *  DROP    := drop table
 *  RECOVER := recover data from somewhere
 *  UPGRADE := become master from slave
 **/
service m2r {
    oneway void create(1: string table, 2: string sql, 3: list<string> region_addrs, 4: i32 aid);
    oneway void drop(1: string table, 2: i32 aid);
    oneway void recover(1: string table, 2: list<string> region_addrs, 3: i32 aid);
    oneway void upgrade(1: string table, 2: list<string> slave_addrs, 3: i32 aid);
}

service r2m {
    oneway void createResp(1: i32 state, 2: i32 aid);
    oneway void dropResp(1: i32 state, 2: i32 aid);
    oneway void recoverResp(1: i32 state, 2: i32 aid);
    oneway void upgradeResp(1: i32 state, 2: i32 aid);
}