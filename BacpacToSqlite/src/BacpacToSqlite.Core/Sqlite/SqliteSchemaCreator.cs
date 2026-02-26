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

    public static int CreateIndices(
        SqliteConnection connection,
        IReadOnlyList<IndexPlan> indices,
        HashSet<string>? includedTables = null)
    {
        int created = 0;
        foreach (var index in indices)
        {
            // Skip indices for tables that weren't included in the conversion
            if (includedTables != null && !includedTables.Contains(index.TableName))
                continue;

            var ddl = GenerateCreateIndex(index);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = ddl;
            try
            {
                cmd.ExecuteNonQuery();
                created++;
            }
            catch (SqliteException)
            {
                // Index creation can fail if table doesn't exist or column names don't match;
                // skip silently since we log the count at the end
            }
        }
        return created;
    }

    public static (int created, int failed) CreateViews(
        SqliteConnection connection,
        IReadOnlyList<ViewPlan> views)
    {
        int created = 0;
        int failed = 0;
        foreach (var view in views)
        {
            var ddl = GenerateCreateView(view);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = ddl;
            try
            {
                cmd.ExecuteNonQuery();
                created++;
            }
            catch (SqliteException)
            {
                // Views using SQL Server-specific syntax will fail; that's expected
                failed++;
            }
        }
        return (created, failed);
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

    public static string GenerateCreateIndex(IndexPlan index)
    {
        var unique = index.IsUnique ? "UNIQUE " : "";
        var columns = string.Join(", ", index.ColumnNames.Select(c => $"\"{c}\""));
        return $"CREATE {unique}INDEX IF NOT EXISTS \"{index.Name}\" ON \"{index.TableName}\" ({columns});";
    }

    public static string GenerateCreateView(ViewPlan view)
    {
        return $"CREATE VIEW IF NOT EXISTS \"{view.Name}\" AS {view.SelectStatement};";
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

            // Decimal/numeric stored as REAL
            "decimal" or "numeric" => "REAL",
            "money" or "smallmoney" => "REAL",

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
