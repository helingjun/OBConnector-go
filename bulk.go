package oceanbase

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// BulkInsert is a helper to perform high-performance multi-row inserts.
// It rewrites multiple single-row inserts into a single multi-row insert.
func BulkInsert(ctx context.Context, db *sql.DB, tableName string, columns []string, values [][]any) (sql.Result, error) {
	if len(values) == 0 {
		return result{affectedRows: 0}, nil
	}

	columnNames := strings.Join(columns, ", ")
	placeholderRow := "(" + strings.Repeat("?, ", len(columns)-1) + "?)"
	
	// OceanBase has a limit on the number of placeholders or packet size.
	// For a professional driver, we should chunk the values.
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
