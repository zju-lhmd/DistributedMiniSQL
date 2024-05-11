/**
 * Client -> Master
 * Operations:
 *  QUERY   := search for table location
 *  CREATE  := create table
 *  DROP    := drop table
 **/
service c2m {
    oneway void query(1: string client_addr, 2: string table);
    oneway void create(1: string client_addr, 2: string table, 3: string sql);
    oneway void drop(1: string client_addr, 2: string table);
}

service m2c {
    oneway void queryResp(1: i32 state, 2: list<string> region_addrs);
    oneway void createResp(1: i32 state, 2: list<string> region_addrs);
    oneway void dropResp(1: i32 state);
}
