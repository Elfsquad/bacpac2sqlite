using System.Text;
using Microsoft.Data.Sqlite;
using BacpacToSqlite.Core.Planning;

namespace BacpacToSqlite.Core.Sqlite;

public static class SqliteSchemaCreator
{
    public static void CreateTables(SqliteConnection connection, IEnumerable<TablePlan> tables)
    {
        foreach (var table in tables)
        {
            var ddl = GenerateCreateTable(table);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = ddl;
            cmd.ExecuteNonQuery();
        }
    }

    public static string GenerateCreateTable(TablePlan table)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE IF NOT EXISTS \"{table.Name}\" (\n");

        for (int i = 0; i < table.Columns.Count; i++)
        {
            var col = table.Columns[i];
            var sqliteType = MapToSqliteType(col.SqlType);
            var nullable = col.IsNullable ? "NULL" : "NOT NULL";

            sb.Append($"    \"{col.Name}\" {sqliteType} {nullable}");

            if (i < table.Columns.Count - 1)
                sb.Append(',');
            sb.Append('\n');
        }

        sb.Append(");");
        return sb.ToString();
    }

    private static string MapToSqliteType(string sqlType)
    {
        return sqlType.ToLowerInvariant() switch
        {
            // Integer types
            "int" or "integer" => "INTEGER",
            "bigint" => "INTEGER",
            "smallint" => "INTEGER",
            "tinyint" => "INTEGER",
            "bit" => "INTEGER",

            // Real types
            "float" => "REAL",
            "real" => "REAL",

            // Text types
            "nvarchar" or "varchar" or "nchar" or "char" or "text" or "ntext" => "TEXT",
            "xml" => "TEXT",

            // Decimal/numeric stored as TEXT to preserve precision
            "decimal" or "numeric" => "TEXT",
            "money" or "smallmoney" => "TEXT",

            // Date/time types stored as TEXT (ISO 8601)
            "date" => "TEXT",
            "time" => "TEXT",
            "datetime" => "TEXT",
            "datetime2" => "TEXT",
            "datetimeoffset" => "TEXT",
            "smalldatetime" => "TEXT",

            // GUID stored as TEXT
            "uniqueidentifier" => "TEXT",

            // Binary types
            "varbinary" or "binary" or "image" => "BLOB",
            "timestamp" or "rowversion" => "BLOB",

            // Spatial / hierarchyid stored as BLOB
            "geometry" or "geography" or "hierarchyid" => "BLOB",

            // sql_variant - use BLOB as catchall
            "sql_variant" => "BLOB",

            // Default to TEXT for unknown types
            _ => "TEXT"
        };
    }
}
