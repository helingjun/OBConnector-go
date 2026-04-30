package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/helingjun/obconnector-go"
)

const defaultQuery = "select 1 from dual"

func main() {
	var attrs repeatedFlag
	var initSQL repeatedFlag
	var (
		dsn       = flag.String("dsn", "", "DSN, for example oceanbase://user:pass@127.0.0.1:2881/db?timeout=5s")
		user      = flag.String("user", "", "OceanBase user, often user@tenant#cluster")
		pass      = flag.String("password", "", "OceanBase password")
		host      = flag.String("host", "127.0.0.1", "OceanBase host or OBProxy host")
		port      = flag.String("port", "2881", "OceanBase port or OBProxy port")
		dbName    = flag.String("database", "", "database/schema name")
		timeout   = flag.Duration("timeout", 10*time.Second, "connect and query timeout")
		trace     = flag.Bool("trace", false, "print handshake and query trace to stderr")
		capAdd    = flag.String("cap-add", "", "capability bits to force on, for example 0x200000")
		capDrop   = flag.String("cap-drop", "", "capability bits to force off, for example 0x100000")
		collation = flag.String("collation", "", "handshake collation id, for example 45")
		preset    = flag.String("preset", "", "client identity preset: default, oboracle, obclient, libobclient, connector-c, connector-j")
		probe     = flag.Bool("probe-presets", false, "try all built-in client identity presets until one succeeds")
		query     = flag.String("query", defaultQuery, "query to execute")
		maxRows   = flag.Int("max-rows", 20, "maximum rows to print for non-default queries")
		txTest    = flag.Bool("tx-test", false, "run a basic begin/query/commit transaction test")
		execTest  = flag.Bool("exec-test", false, "run a DDL/DML ExecContext smoke test with a temporary table")
		execTable = flag.String("exec-table", "", "table name for -exec-test; defaults to a generated OBGO_SMOKE_* name")
		paramTest = flag.Bool("param-test", false, "run parameterized QueryContext/ExecContext smoke tests")
		poolTest  = flag.Bool("pool-test", false, "run database/sql pool lifecycle smoke tests")
	)
	flag.Var(&attrs, "attr", "connection attribute key=value; can be repeated")
	flag.Var(&initSQL, "init", "initial SQL to run after auth; can be repeated")
	flag.Parse()

	connString := *dsn
	if connString == "" {
		if *user == "" {
			fmt.Fprintln(os.Stderr, "missing -user or -dsn")
			os.Exit(2)
		}
		connString = buildDSN(*user, *pass, *host, *port, *dbName, *timeout, *trace, *capAdd, *capDrop, *collation, *preset, attrs, initSQL)
	} else {
		var err error
		connString, err = applyExperimentParams(connString, *trace, *capAdd, *capDrop, *collation, *preset, attrs, initSQL)
		if err != nil {
			exitErr(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if *probe {
		if err := probePresets(ctx, connString); err != nil {
			exitErr(err)
		}
		return
	}

	if *txTest {
		if err := runTxTest(ctx, connString); err != nil {
			exitErr(err)
		}
		return
	}

	if *execTest {
		if err := runExecTest(ctx, connString, *execTable); err != nil {
			exitErr(err)
		}
		return
	}

	if *paramTest {
		if err := runParamTest(ctx, connString, *execTable); err != nil {
			exitErr(err)
		}
		return
	}

	if *poolTest {
		if err := runPoolTest(ctx, connString); err != nil {
			exitErr(err)
		}
		return
	}

	if *query == defaultQuery {
		value, err := runScalar(ctx, connString, *query)
		if err != nil {
			exitErr(err)
		}
		fmt.Printf("ok: %s => %v\n", *query, value)
		return
	}

	if err := runQuery(ctx, connString, *query, *maxRows); err != nil {
		exitErr(err)
	}
}

func openDB(connString string) (*sql.DB, error) {
	db, err := sql.Open("oceanbase", connString)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func runScalar(ctx context.Context, connString string, query string) (any, error) {
	db, err := openDB(connString)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var value any
	if err := db.QueryRowContext(ctx, query).Scan(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func probePresets(ctx context.Context, baseDSN string) error {
	presets := []string{"default", "oboracle", "obclient", "libobclient", "connector-c", "connector-j"}
	var lastErr error
	for _, preset := range presets {
		dsn, err := applyExperimentParams(baseDSN, false, "", "", "", preset, nil, nil)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "== probe preset %s ==\n", preset)
		value, err := runScalar(ctx, dsn, defaultQuery)
		if err == nil {
			fmt.Printf("ok: preset=%s select 1 from dual => %v\n", preset, value)
			return nil
		}
		fmt.Fprintf(os.Stderr, "preset %s failed: %v\n", preset, err)
		lastErr = err
	}
	return lastErr
}

func runQuery(ctx context.Context, connString string, query string, maxRows int) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	for i, column := range columns {
		typeName := ""
		if i < len(columnTypes) {
			typeName = columnTypes[i].DatabaseTypeName()
		}
		if i > 0 {
			fmt.Print("\t")
		}
		if typeName == "" {
			fmt.Print(column)
		} else {
			fmt.Printf("%s(%s)", column, typeName)
		}
	}
	fmt.Println()

	values := make([]any, len(columns))
	scan := make([]any, len(columns))
	for i := range values {
		scan[i] = &values[i]
	}

	count := 0
	for rows.Next() {
		if err := rows.Scan(scan...); err != nil {
			return err
		}
		if maxRows <= 0 || count < maxRows {
			printRow(values)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if maxRows > 0 && count > maxRows {
		fmt.Printf("... truncated, printed %d of %d rows\n", maxRows, count)
	} else {
		fmt.Printf("rows: %d\n", count)
	}
	return nil
}

func runTxTest(ctx context.Context, connString string) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var value any
	if err := tx.QueryRowContext(ctx, defaultQuery).Scan(&value); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	fmt.Printf("ok: tx %s => %v\n", defaultQuery, value)
	return nil
}

func runExecTest(ctx context.Context, connString string, tableName string) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()

	tableName, err = smokeTableName(tableName)
	if err != nil {
		return err
	}
	fmt.Printf("exec-test table: %s\n", tableName)

	if err := dropTableIgnoreMissing(ctx, db, tableName); err != nil {
		return err
	}
	defer func() {
		if err := dropTableIgnoreMissing(context.Background(), db, tableName); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup warning: %v\n", err)
		}
	}()

	if _, err := execStep(ctx, db, "create", fmt.Sprintf("create table %s (id number, name varchar2(40))", tableName)); err != nil {
		return err
	}
	if _, err := execStep(ctx, db, "insert-1", fmt.Sprintf("insert into %s (id, name) values (1, 'alpha')", tableName)); err != nil {
		return err
	}
	if _, err := execStep(ctx, db, "insert-2", fmt.Sprintf("insert into %s (id, name) values (2, 'beta')", tableName)); err != nil {
		return err
	}
	if _, err := execStep(ctx, db, "update", fmt.Sprintf("update %s set name = 'gamma' where id = 2", tableName)); err != nil {
		return err
	}

	var count any
	if err := db.QueryRowContext(ctx, fmt.Sprintf("select count(*) from %s", tableName)).Scan(&count); err != nil {
		return err
	}
	fmt.Printf("select-count: %v\n", formatValue(count))

	if _, err := execStep(ctx, db, "delete", fmt.Sprintf("delete from %s where id = 1", tableName)); err != nil {
		return err
	}
	if _, err := execStep(ctx, db, "drop", fmt.Sprintf("drop table %s", tableName)); err != nil {
		return err
	}
	fmt.Println("ok: exec-test completed")
	return nil
}

func runParamTest(ctx context.Context, connString string, tableName string) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()

	var n, s, x any
	if err := db.QueryRowContext(ctx, "select ? as n, ? as s, ? as x from dual", int64(123), "O'Reilly", nil).Scan(&n, &s, &x); err != nil {
		return fmt.Errorf("param select failed: %w", err)
	}
	fmt.Printf("param-select: n=%s s=%s x=%s\n", formatValue(n), formatValue(s), formatValue(x))

	tableName, err = smokeTableName(tableName)
	if err != nil {
		return err
	}
	fmt.Printf("param-test table: %s\n", tableName)

	if err := dropTableIgnoreMissing(ctx, db, tableName); err != nil {
		return err
	}
	defer func() {
		if err := dropTableIgnoreMissing(context.Background(), db, tableName); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup warning: %v\n", err)
		}
	}()

	if _, err := execStep(ctx, db, "create", fmt.Sprintf("create table %s (id number, name varchar2(80), created_at timestamp)", tableName)); err != nil {
		return err
	}
	when := time.Date(2026, 4, 30, 11, 56, 0, 123456000, time.UTC)
	if _, err := execStepArgs(ctx, db, "param-insert-1", fmt.Sprintf("insert into %s (id, name, created_at) values (?, ?, ?)", tableName), int64(1), "alpha", when); err != nil {
		return err
	}
	if _, err := execStepArgs(ctx, db, "param-insert-2", fmt.Sprintf("insert into %s (id, name, created_at) values (?, ?, ?)", tableName), int64(2), "beta's", nil); err != nil {
		return err
	}
	if _, err := execStepArgs(ctx, db, "param-update", fmt.Sprintf("update %s set name = ? where id = ?", tableName), "gamma", int64(2)); err != nil {
		return err
	}

	var count any
	if err := db.QueryRowContext(ctx, fmt.Sprintf("select count(*) from %s where name = ?", tableName), "gamma").Scan(&count); err != nil {
		return fmt.Errorf("param count failed: %w", err)
	}
	fmt.Printf("param-count: %s\n", formatValue(count))

	if _, err := execStep(ctx, db, "drop", fmt.Sprintf("drop table %s", tableName)); err != nil {
		return err
	}
	fmt.Println("ok: param-test completed")
	return nil
}

func runPoolTest(ctx context.Context, connString string) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	fmt.Println("pool-test ping: ok")

	firstID, err := queryOnDedicatedConn(ctx, db)
	if err != nil {
		return err
	}
	secondID, err := queryOnDedicatedConn(ctx, db)
	if err != nil {
		return err
	}
	fmt.Printf("pool-test idle-reuse: first=%s second=%s reused=%t\n", firstID, secondID, firstID == secondID)
	if firstID != secondID {
		return fmt.Errorf("idle connection was not reused")
	}

	if err := closeOneDriverConn(ctx, db); err != nil {
		return err
	}
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("bad connection retry ping failed: %w", err)
	}
	fmt.Println("pool-test bad-conn-retry: ok")

	if err := runConcurrentQueries(ctx, db, 8); err != nil {
		return err
	}
	stats := db.Stats()
	fmt.Printf("pool-test stats: open=%d in_use=%d idle=%d wait_count=%d\n", stats.OpenConnections, stats.InUse, stats.Idle, stats.WaitCount)
	fmt.Println("ok: pool-test completed")
	return nil
}

func queryOnDedicatedConn(ctx context.Context, db *sql.DB) (string, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("get dedicated conn failed: %w", err)
	}
	defer conn.Close()

	id, err := rawConnID(ctx, conn)
	if err != nil {
		return "", err
	}
	var value any
	if err := conn.QueryRowContext(ctx, defaultQuery).Scan(&value); err != nil {
		return "", fmt.Errorf("dedicated conn query failed: %w", err)
	}
	fmt.Printf("pool-test dedicated-query: conn=%s value=%s\n", id, formatValue(value))
	return id, nil
}

func rawConnID(ctx context.Context, conn *sql.Conn) (string, error) {
	var id string
	err := conn.Raw(func(driverConn any) error {
		id = fmt.Sprintf("%p", driverConn)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("raw conn id failed: %w", err)
	}
	return id, nil
}

func closeOneDriverConn(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get conn for bad-conn test failed: %w", err)
	}
	defer conn.Close()

	var id string
	if err := conn.Raw(func(driverConn any) error {
		id = fmt.Sprintf("%p", driverConn)
		closer, ok := driverConn.(interface{ Close() error })
		if !ok {
			return fmt.Errorf("driver conn %T has no Close", driverConn)
		}
		return closer.Close()
	}); err != nil {
		return fmt.Errorf("close raw driver conn failed: %w", err)
	}
	fmt.Printf("pool-test bad-conn-close: conn=%s closed\n", id)
	return nil
}

func runConcurrentQueries(ctx context.Context, db *sql.DB, workers int) error {
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var value any
			if err := db.QueryRowContext(ctx, "select ? from dual", int64(worker+1)).Scan(&value); err != nil {
				errCh <- fmt.Errorf("worker %d query failed: %w", worker, err)
				return
			}
			fmt.Printf("pool-test concurrent worker=%d value=%s\n", worker, formatValue(value))
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	fmt.Printf("pool-test concurrent: workers=%d ok\n", workers)
	return nil
}

func execStep(ctx context.Context, db *sql.DB, name string, query string) (sql.Result, error) {
	return execStepArgs(ctx, db, name, query)
}

func execStepArgs(ctx context.Context, db *sql.DB, name string, query string, args ...any) (sql.Result, error) {
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s failed: %w", name, err)
	}
	affected, affectedErr := res.RowsAffected()
	insertID, insertErr := res.LastInsertId()
	fmt.Printf("%s: rows_affected=%s last_insert_id=%s\n", name, resultValue(affected, affectedErr), resultValue(insertID, insertErr))
	return res, nil
}

func resultValue(value int64, err error) string {
	if err != nil {
		return "unknown"
	}
	return fmt.Sprint(value)
}

func dropTableIgnoreMissing(ctx context.Context, db *sql.DB, tableName string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("drop table %s", tableName))
	if err == nil || isMissingTableError(err) {
		return nil
	}
	return err
}

func isMissingTableError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "ORA-00942") || strings.Contains(msg, "error 942")
}

func smokeTableName(name string) (string, error) {
	if name == "" {
		return fmt.Sprintf("OBGO_SMOKE_%06X", time.Now().UnixNano()&0xffffff), nil
	}
	if !validIdentifier.MatchString(name) {
		return "", fmt.Errorf("invalid table name %q", name)
	}
	return strings.ToUpper(name), nil
}

var validIdentifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,29}$`)

func printRow(values []any) {
	for i, value := range values {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(formatValue(value))
	}
	fmt.Println()
}

func formatValue(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(v)
	}
}

func discardRows(rows *sql.Rows) error {
	defer rows.Close()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil && err != io.EOF {
		return err
	}
	return nil
}

type repeatedFlag []string

func (f *repeatedFlag) String() string {
	if f == nil {
		return ""
	}
	return strings.Join(*f, ",")
}

func (f *repeatedFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func buildDSN(user, password, host, port, database string, timeout time.Duration, trace bool, capAdd, capDrop, collation, preset string, attrs, initSQL []string) string {
	u := &url.URL{
		Scheme: "oceanbase",
		User:   url.UserPassword(user, password),
		Host:   net.JoinHostPort(host, port),
		Path:   database,
	}
	values, _ := experimentValues(trace, capAdd, capDrop, collation, preset, attrs, initSQL)
	values.Set("timeout", timeout.String())
	u.RawQuery = values.Encode()
	return u.String()
}

func applyExperimentParams(dsn string, trace bool, capAdd, capDrop, collation, preset string, attrs, initSQL []string) (string, error) {
	values, changed := experimentValues(trace, capAdd, capDrop, collation, preset, attrs, initSQL)
	if !strings.Contains(dsn, "://") {
		if strings.HasPrefix(dsn, "oceanbase:") || strings.HasPrefix(dsn, "oboracle:") {
			if !changed {
				return dsn, nil
			}
			return appendRawQuery(dsn, values), nil
		}
		if changed {
			return "", fmt.Errorf("experiment flags with -dsn require URL-style or oceanbase: DSN")
		}
		return dsn, nil
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	existing := u.Query()
	for key, vals := range values {
		for _, value := range vals {
			existing.Add(key, value)
		}
	}
	u.RawQuery = existing.Encode()
	return u.String(), nil
}

func experimentValues(trace bool, capAdd, capDrop, collation, preset string, attrs, initSQL []string) (url.Values, bool) {
	values := url.Values{}
	if trace {
		values.Set("trace", "true")
	}
	if capAdd != "" {
		values.Set("cap.add", capAdd)
	}
	if capDrop != "" {
		values.Set("cap.drop", capDrop)
	}
	if collation != "" {
		values.Set("collation", collation)
	}
	if preset != "" {
		values.Set("preset", preset)
	}
	for _, attr := range attrs {
		key, value, ok := strings.Cut(attr, "=")
		if !ok || key == "" {
			fmt.Fprintf(os.Stderr, "ignore malformed -attr %q, expected key=value\n", attr)
			continue
		}
		values.Set("attr."+key, value)
	}
	for _, query := range initSQL {
		values.Add("init", query)
	}
	return values, len(values) > 0
}

func appendRawQuery(dsn string, values url.Values) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + values.Encode()
}

func exitErr(err error) {
	fmt.Fprintf(os.Stderr, "obping: %v\n", err)
	os.Exit(1)
}
