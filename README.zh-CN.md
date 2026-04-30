# obconnector-go

[English](README.md)

`obconnector-go` 是一个面向 Go `database/sql` 的 OceanBase 驱动实验项目，目标是在纯 Go、无 cgo、无本地动态库依赖的前提下，连接 OceanBase Oracle 租户。

第一个里程碑已经完成：通过 OceanBase Oracle 租户认证，并执行：

```sql
select 1 from dual
```

本项目不依赖 OBCI、Oracle Instant Client、LibOBClient 或 OBConnector-C 动态库。

## 当前状态

驱动目前注册两个 `database/sql` driver name：

- `oceanbase`
- `oboracle`

已实现的能力：

- OceanBase Oracle 租户握手扩展。
- `mysql_native_password` 认证。
- `COM_QUERY` 查询路径。
- 流式读取文本协议结果集，避免大结果集一次性进入内存。
- `QueryContext` / `ExecContext`。
- `RowsAffected()`。
- 基础事务：`BeginTx`、`Commit`、`Rollback`。
- `Prepare` / `PrepareContext` 兼容路径。
- 客户端侧 `?` 参数插值兼容层。
- 服务端错误包解析，保留错误码、SQLSTATE 和 ORA 消息。
- `database/sql` 连接池生命周期接口：`IsValid`、`ResetSession`。
- 基础列类型元数据。
- `cmd/obping` 连接和协议验证工具。

注意：当前仍是早期驱动，不是完整生产版本。Oracle 模式下文本协议返回的列类型码可能不可靠，例如 `sysdate` 可能被标记为 `DECIMAL`，因此当前默认保守地把文本结果作为字符串返回。

## 安装

```bash
go get github.com/helingjun/obconnector-go
```

在代码中使用：

```go
package main

import (
	"database/sql"

	_ "github.com/helingjun/obconnector-go"
)

func main() {
	db, err := sql.Open("oceanbase", "oceanbase://user:password@127.0.0.1:2881/?timeout=5s")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	var one string
	if err := db.QueryRow("select 1 from dual").Scan(&one); err != nil {
		panic(err)
	}
}
```

## DSN

推荐 URL 风格 DSN：

```text
oceanbase://user:password@host:port/database?timeout=5s
```

也兼容实验中常见的 opaque DSN：

```text
oceanbase:user:password@host:port/database?TIMEOUT=10
```

常用参数：

- `timeout=5s`：连接和读写超时。
- `trace=true`：输出握手、能力位、连接属性和查询日志。
- `attr.<key>=<value>`：追加或覆盖连接属性。
- `cap.add=<uint32>`：强制打开能力位。
- `cap.drop=<uint32>`：强制关闭能力位。
- `collation=<uint8>`：设置握手 collation。
- `init=<sql>`：认证后执行初始化 SQL，可重复。
- `preset=<name>`：切换客户端标识预设。默认预设会发送 OceanBase Connector/C 风格的 Oracle 租户连接属性。

## obping

`cmd/obping` 是验证驱动和协议行为的命令行工具。

基础连接测试：

```bash
go run ./cmd/obping \
  -host 127.0.0.1 \
  -port 2881 \
  -user 'user@tenant#cluster' \
  -password '<password>' \
  -trace
```

自定义查询：

```bash
go run ./cmd/obping \
  -dsn 'oceanbase://user:password@127.0.0.1:2881/?timeout=5s' \
  -query 'select 1 as one, sysdate as now from dual' \
  -max-rows 20
```

事务验证：

```bash
go run ./cmd/obping \
  -dsn 'oceanbase://user:password@127.0.0.1:2881/?timeout=5s' \
  -tx-test
```

DDL/DML 验证：

```bash
go run ./cmd/obping \
  -dsn 'oceanbase://user:password@127.0.0.1:2881/?timeout=5s' \
  -exec-test
```

参数插值验证：

```bash
go run ./cmd/obping \
  -dsn 'oceanbase://user:password@127.0.0.1:2881/?timeout=5s' \
  -param-test
```

## OceanBase Oracle 租户关键发现

普通 MySQL Go 驱动连接 OceanBase Oracle 租户时，可能被服务端拒绝并返回：

```text
Oracle tenant for current client driver is not supported
```

当前 PoC 成功所需的关键协议行为：

- 握手响应中发送 `CLIENT_SUPPORT_ORACLE_MODE`。
- 该扩展位不能因为服务端未在标准 MySQL capability bitmap 中显式声明就被过滤掉。
- 连接属性中发送 OceanBase Connector/C 风格字段：
  - `__mysql_client_type=__ob_libobclient`
  - `__ob_client_name=OceanBase Connector/C`
  - `__proxy_capability_flag=311552`
  - `__ob_client_attribute_capability_flag=5`

详细协议记录见 `docs/protocol-notes.md`。

## 参数支持说明

当前 `?` 参数支持是客户端侧插值兼容层，不是服务端 prepared statement。

支持的参数类型：

- `nil`
- `string`
- `int64`
- `float64`
- `bool`
- `[]byte`
- `time.Time`

插值逻辑会跳过字符串字面量、双引号标识符、行注释和块注释中的 `?`。

## 安全和隐私

请不要提交真实的：

- 数据库密码。
- 私有主机名或内网 IP。
- 租户名、集群名、用户名。
- TLS 私钥、证书、token。
- 未脱敏的抓包文件或日志。

公开 issue、PR 和协议记录中请使用占位符，并脱敏敏感字段。

## Clean-room 约束

本项目可以参考公开文档、自己环境中的抓包结果和外部客户端的高层行为，但不要复制 OBConnector-C、MariaDB Connector/C 或其他不兼容许可证项目的源码、注释、变量命名和控制流。

协议发现请记录在 `docs/protocol-notes.md`。

## 开发

提交前请运行：

```bash
gofmt -w .
go test ./...
go vet ./...
```

## 许可证

Apache-2.0。详见 `LICENSE`。
