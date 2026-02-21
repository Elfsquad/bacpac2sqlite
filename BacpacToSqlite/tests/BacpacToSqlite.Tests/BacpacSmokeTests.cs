using BacpacToSqlite.Core;
using BacpacToSqlite.Core.BcpNative;
using BacpacToSqlite.Core.Sqlite;
using Microsoft.Data.Sqlite;

namespace BacpacToSqlite.Tests;

public class BacpacSmokeTests
{
    [Fact]
    public void SqliteBulkInserter_InsertsRows()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = "CREATE TABLE Test (Id INTEGER, Name TEXT)";
        createCmd.ExecuteNonQuery();

        var plan = new Core.Planning.TablePlan
        {
            Schema = "dbo",
            Name = "Test",
            Columns =
            [
                new() { Name = "Id", Ordinal = 0, SqlType = "int", IsNullable = false },
                new() { Name = "Name", Ordinal = 1, SqlType = "nvarchar", IsNullable = true, MaxLength = 100 }
            ]
        };

        using var inserter = new SqliteBulkInserter(connection, pragmaFast: false, foreignKeysOff: false);

        object?[][] rows =
        [
            [1L, "Alice"],
            [2L, "Bob"],
            [3L, null]
        ];

        var count = inserter.InsertRows(plan, rows);
        Assert.Equal(3, count);

        var sqliteCount = inserter.GetRowCount("Test");
        Assert.Equal(3, sqliteCount);
    }

    [Fact]
    public void SqliteSchemaVerifier_PassesValidSchema()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = "CREATE TABLE MyTable (Id INTEGER, Value TEXT)";
        createCmd.ExecuteNonQuery();

        var plan = new Core.Planning.TablePlan
        {
            Schema = "dbo",
            Name = "MyTable",
            Columns =
            [
                new() { Name = "Id", Ordinal = 0, SqlType = "int", IsNullable = false },
                new() { Name = "Value", Ordinal = 1, SqlType = "nvarchar", IsNullable = true }
            ]
        };

        // Should not throw
        SqliteSchemaVerifier.Verify(connection, [plan]);
    }

    [Fact]
    public void SqliteSchemaVerifier_ThrowsOnMissingTable()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();

        var plan = new Core.Planning.TablePlan
        {
            Schema = "dbo",
            Name = "NonExistent",
            Columns =
            [
                new() { Name = "Id", Ordinal = 0, SqlType = "int", IsNullable = false }
            ]
        };

        Assert.Throws<InvalidOperationException>(() =>
            SqliteSchemaVerifier.Verify(connection, [plan]));
    }

    [Fact]
    public void BcpFormatInference_DefaultsToUnicodeNative()
    {
        var plan = new Core.Planning.TablePlan
        {
            Schema = "dbo",
            Name = "Test",
            Columns =
            [
                new() { Name = "Id", Ordinal = 0, SqlType = "int", IsNullable = false },
                new() { Name = "Name", Ordinal = 1, SqlType = "nvarchar", IsNullable = true, MaxLength = 100 }
            ]
        };

        var format = BcpFormatInference.InferOrAutoDetect(plan);
        Assert.Equal(BcpProfile.UnicodeNative, format.Profile);
        Assert.Equal(2, format.Columns.Count);
        Assert.Equal(BcpTypeCategory.FixedInt, format.Columns[0].TypeCategory);
        Assert.Equal(BcpTypeCategory.UnicodeString, format.Columns[1].TypeCategory);
    }
}
