package database

import (
	"testing"

	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestDBTypeToStorageType(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
		want     storage.DataType
	}{
		// Integers across Postgres/MySQL spellings.
		{"postgres integer", "integer", storage.TypeBigInt},
		{"postgres int4", "int4", storage.TypeBigInt},
		{"postgres int8", "int8", storage.TypeBigInt},
		{"bigint", "bigint", storage.TypeBigInt},
		{"smallint", "smallint", storage.TypeBigInt},
		{"mysql int", "int", storage.TypeBigInt},
		{"mysql tinyint", "tinyint", storage.TypeBigInt},
		{"uppercase INT", "INT", storage.TypeBigInt},
		{"padded", "  BIGINT  ", storage.TypeBigInt},

		// Floating point / decimal.
		{"numeric", "numeric", storage.TypeDouble},
		{"decimal", "decimal", storage.TypeDouble},
		{"double precision", "double precision", storage.TypeDouble},
		{"real", "real", storage.TypeDouble},
		{"float", "float", storage.TypeDouble},

		// Booleans (checked before "int" so they never become BIGINT).
		{"boolean", "boolean", storage.TypeBoolean},
		{"bool", "bool", storage.TypeBoolean},

		// Everything else falls back to VARCHAR.
		{"varchar", "character varying", storage.TypeVarchar},
		{"text", "text", storage.TypeVarchar},
		{"timestamp", "timestamp without time zone", storage.TypeVarchar},
		{"date", "date", storage.TypeVarchar},
		{"json", "json", storage.TypeVarchar},
		{"empty", "", storage.TypeVarchar},
		{"unknown", "geometry", storage.TypeVarchar},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, dbTypeToStorageType(tt.dataType))
		})
	}
}
