using Microsoft.Data.Sqlite;
using BacpacToSqlite.Core.Planning;

namespace BacpacToSqlite.Core.Sqlite;

public static class SqliteSchemaVerifier
{
    public static void Verify(SqliteConnection connection, IEnumerable<TablePlan> tables)
    {
        foreach (var table in tables)
        {
            VerifyTable(connection, table);
        }
    }

    private static void VerifyTable(SqliteConnection connection, TablePlan table)
    {
        // Check table exists
        var tableName = table.Name;
        using var checkCmd = connection.CreateCommand();
        checkCmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name=@name COLLATE NOCASE";
        checkCmd.Parameters.AddWithValue("@name", tableName);

        var result = checkCmd.ExecuteScalar();
        if (result == null)
        {
            throw new InvalidOperationException(
                $"Table '{tableName}' not found in SQLite database. " +
                $"Ensure the schema is created before running the import.");
        }

        // Get column info
        using var infoCmd = connection.CreateCommand();
        infoCmd.CommandText = $"PRAGMA table_info(\"{tableName}\")";

        var sqliteColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var reader = infoCmd.ExecuteReader();
        while (reader.Read())
        {
            sqliteColumns.Add(reader.GetString(1)); // column name
        }

        // Verify all plan columns exist
        var missingColumns = table.Columns
            .Where(c => !sqliteColumns.Contains(c.Name))
            .Select(c => c.Name)
            .ToList();

        if (missingColumns.Count > 0)
        {
            throw new InvalidOperationException(
                $"Table '{tableName}' is missing columns: {string.Join(", ", missingColumns)}");
        }
    }
}
