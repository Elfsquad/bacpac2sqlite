using Microsoft.Data.Sqlite;
using BacpacToSqlite.Core.Planning;

namespace BacpacToSqlite.Core.Sqlite;

public sealed class SqliteBulkInserter : IDisposable
{
    private readonly SqliteConnection _connection;
    private readonly bool _pragmaFast;
    private readonly bool _foreignKeysOff;
    private readonly bool _truncate;
    private readonly int _batchSize;

    public SqliteBulkInserter(
        SqliteConnection connection,
        bool pragmaFast = true,
        bool foreignKeysOff = true,
        bool truncate = false,
        int batchSize = 5000)
    {
        _connection = connection;
        _pragmaFast = pragmaFast;
        _foreignKeysOff = foreignKeysOff;
        _truncate = truncate;
        _batchSize = batchSize;
    }

    public void ApplyPragmas()
    {
        if (_pragmaFast)
        {
            Execute("PRAGMA journal_mode=WAL;");
            Execute("PRAGMA synchronous=OFF;");
            Execute("PRAGMA temp_store=MEMORY;");
        }

        if (_foreignKeysOff)
        {
            Execute("PRAGMA foreign_keys=OFF;");
        }
    }

    public long InsertRows(TablePlan table, IEnumerable<object?[]> rows)
    {
        var tableName = table.Name;
        var columns = table.Columns;

        if (_truncate)
        {
            Execute($"DELETE FROM \"{tableName}\";");
        }

        var columnList = string.Join(", ", columns.Select(c => $"\"{c.Name}\""));
        var paramList = string.Join(", ", columns.Select((_, i) => $"@p{i}"));
        var sql = $"INSERT INTO \"{tableName}\" ({columnList}) VALUES ({paramList})";

        long rowCount = 0;
        SqliteTransaction? transaction = null;

        try
        {
            transaction = _connection.BeginTransaction();

            using var cmd = _connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = sql;

            // Pre-create parameters
            var parameters = new SqliteParameter[columns.Count];
            for (int i = 0; i < columns.Count; i++)
            {
                parameters[i] = new SqliteParameter($"@p{i}", null);
                cmd.Parameters.Add(parameters[i]);
            }

            foreach (var row in rows)
            {
                for (int i = 0; i < columns.Count; i++)
                {
                    parameters[i].Value = row[i] ?? DBNull.Value;
                }

                cmd.ExecuteNonQuery();
                rowCount++;

                if (rowCount % _batchSize == 0)
                {
                    transaction.Commit();
                    transaction.Dispose();
                    transaction = _connection.BeginTransaction();
                    cmd.Transaction = transaction;
                }
            }

            transaction.Commit();
        }
        catch
        {
            transaction?.Rollback();
            throw;
        }
        finally
        {
            transaction?.Dispose();
        }

        return rowCount;
    }

    public long GetRowCount(string tableName)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM \"{tableName}\"";
        return (long)cmd.ExecuteScalar()!;
    }

    public void RestoreForeignKeys()
    {
        if (_foreignKeysOff)
        {
            Execute("PRAGMA foreign_keys=ON;");
        }
    }

    private void Execute(string sql)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    public void Dispose() { }
}
