/**
 * Region -> Region
 * Operations:
 *  SYNC    := update slaves' data (normal case)
 *  COPY    := copy data to another region
 **/
service r2r {
    oneway void sync(1: string sql);
    oneway void copy(1: string table);

    oneway void syncResp(1: i32 state);
    oneway void copyResp(1: i32 state, 2: string dump);
}
