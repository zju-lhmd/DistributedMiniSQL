/**
 * Region -> Region
 * Operations:
 *  SYNC    := update slaves' data (normal case)
 *  COPY    := copy data to another region
 **/
service r2r {
    oneway void sync(1: string sql);
    oneway void copy(1: string name, 2: binary buff);
}
