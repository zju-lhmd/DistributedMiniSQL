/**
 * Common definitions
 **/
exception Error {
    1: i32 code;
    2: string msg;
}

// schema/records are seperated by whitespace
struct Hits {
    1: list<string> schema;
    2: list<string> records;
}
