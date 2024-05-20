/**
 * Client -> Region
 * Operations:
 *  read     := execute SQL like select
 *  write    := execute SQL like insert, update, delete
 **/
struct Hits {
    1: string schema;
    2: list<string> records;
}

service c2r {
    oneway void read(1: string client_addr, 2: string sql);
    oneway void write(1: string client_addr, 2: string table, 3: string sql);
}

service r2c {
    oneway void readResp(1: i32 state, 2: Hits hits);
    oneway void writeResp(1: i32 state, 2: string message);
}
