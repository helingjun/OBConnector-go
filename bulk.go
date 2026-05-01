package oceanbase

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
)

type bulkExecer interface {
	BulkExecContext(ctx context.Context, argRows [][]driver.NamedValue) (driver.Result, error)
}

// BulkInsert is a helper to perform high-performance multi-row inserts.
// It uses native COM_STMT_BULK_EXECUTE if supported, otherwise rewrites the SQL.
func BulkInsert(ctx context.Context, db *sql.DB, tableName string, columns []string, values [][]any) (sql.Result, error) {
	if len(values) == 0 {
		return result{affectedRows: 0}, nil
	}

	// Try native bulk execution first
	res, err := tryNativeBulkInsert(ctx, db, tableName, columns, values)
	if err == nil {
		return res, nil
	}
	// Fallback to SQL rewriting
	return bulkInsertRewrite(ctx, db, tableName, columns, values)
}

func tryNativeBulkInsert(ctx context.Context, db *sql.DB, tableName string, columns []string, values [][]any) (sql.Result, error) {
	columnNames := strings.Join(columns, ", ")
	placeholders := "(" + strings.Repeat("?, ", len(columns)-1) + "?)"
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, columnNames, placeholders)

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var result sql.Result
	err = conn.Raw(func(driverConn any) error {
		dConn, ok := driverConn.(driver.ConnPrepareContext)
		if !ok {
			return fmt.Errorf("driver does not support ConnPrepareContext")
		}
		stmt, err := dConn.PrepareContext(ctx, query)
		if err != nil {
			return err
		}
		defer stmt.Close()

		bExec, ok := stmt.(bulkExecer)
		if !ok {
			return fmt.Errorf("statement does not support BulkExecContext")
		}

		argRows := make([][]driver.NamedValue, len(values))
		for i, row := range values {
			named := make([]driver.NamedValue, len(row))
			for j, val := range row {
				named[j] = driver.NamedValue{Ordinal: j + 1, Value: val}
			}
			argRows[i] = named
		}

		res, err := bExec.BulkExecContext(ctx, argRows)
		if err != nil {
			return err
		}
		result = res
		return nil
	})

	return result, err
}

func bulkInsertRewrite(ctx context.Context, db *sql.DB, tableName string, columns []string, values [][]any) (sql.Result, error) {
	columnNames := strings.Join(columns, ", ")
	placeholderRow := "(" + strings.Repeat("?, ", len(columns)-1) + "?)"
	
	const maxChunkSize = 1000 
	
	var totalAffected int64
	for i := 0; i < len(values); i += maxChunkSize {
		end := i + maxChunkSize
		if end > len(values) {
			end = len(values)
		}
		
		chunk := values[i:end]
		placeholders := make([]string, len(chunk))
		for j := range chunk {
			placeholders[j] = placeholderRow
		}
		
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, columnNames, strings.Join(placeholders, ", "))
		
		flattenedValues := make([]any, 0, len(chunk)*len(columns))
		for _, row := range chunk {
			flattenedValues = append(flattenedValues, row...)
		}
		
		res, err := db.ExecContext(ctx, query, flattenedValues...)
		if err != nil {
			return nil, err
		}
		
		affected, _ := res.RowsAffected()
		totalAffected += affected
	}
	
	return result{affectedRows: totalAffected}, nil
}
