/* 标记请求发送方 */
enum From {
	DDB_MASTER = 1,
	DDB_REGION = 2,
	DDB_CLIENT = 3,
}

/* 操作指令 */
enum Op {
	DDB_CREATE = 1,
	DDB_DROP = 2,
	DDB_QUERY = 3,
	DDB_WRITE = 4,
	DDB_RECOVER = 5,
}

/* 请求数据格式 */
struct DRequest {
	1: From from,
	2: Op op,
	3: list<string> params,
}

/* 回复数据格式 */
struct DResponse {
	1: list<string> results,
	2: bool more,	// more == true 表示还有更多数据未发送
}

/* 错误数据格式 */
exception DException {
	1: required string err;
}

/* 请求处理器 */
service DHandler {
	DResponse handle(1: DRequest req) throws (1: DException e),
}
