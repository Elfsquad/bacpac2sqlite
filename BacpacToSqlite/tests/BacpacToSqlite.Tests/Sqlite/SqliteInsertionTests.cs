using BacpacToSqlite.Tests.Planning;
using FluentAssertions;
using Microsoft.Data.Sqlite;

namespace BacpacToSqlite.Tests.Sqlite;

public class SqliteInsertionTests
{
    [Fact]
    public void InsertRows_BindsNullEmptyBlobGuidAndDecimalCorrectly()
    {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = """
            CREATE TABLE "KitchenSink" (
                "Id" INTEGER NOT NULL,
                "TenantId" TEXT NOT NULL,
                "CreatedAt" TEXT NOT NULL,
                "Price" TEXT NOT NULL,
                "Payload" BLOB NULL,
                "Title" TEXT NULL,
                "IsActive" INTEGER NOT NULL,
                "Big" INTEGER NULL
            );
            """;
            cmd.ExecuteNonQuery();
        }

        var plan = TestPlans.KitchenSink();

        // Data matches our decoder output format: strings for guid/datetime/decimal
        var rows = new List<object?[]>
        {
            new object?[] { 1L, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "2024-01-02T03:04:05.1234567Z", "123.4567", null, null, 1L, null },
            new object?[] { 2L, "ffffffff-1111-2222-3333-444444444444", "2024-01-02T03:04:05.0000000Z", "0.0001", new byte[]{0xDE, 0xAD}, "", 0L, 9223372036854775807L },
        };

        CoreAdapters.InsertRows(conn, plan, rows);

        using var q = conn.CreateCommand();
        q.CommandText = """SELECT Id, TenantId, CreatedAt, Price, length(Payload), Title, IsActive, Big FROM KitchenSink ORDER BY Id;""";

        using var r = q.ExecuteReader();

        // Row 1
        r.Read().Should().BeTrue();
        r.GetInt64(0).Should().Be(1);
        r.GetString(1).Should().Be("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        r.GetString(2).Should().Be("2024-01-02T03:04:05.1234567Z");
        r.GetString(3).Should().Be("123.4567");
        r.IsDBNull(4).Should().BeTrue();  // NULL payload
        r.IsDBNull(5).Should().BeTrue();  // NULL title
        r.GetInt64(6).Should().Be(1);
        r.IsDBNull(7).Should().BeTrue();  // NULL big

        // Row 2
        r.Read().Should().BeTrue();
        r.GetInt64(0).Should().Be(2);
        r.GetString(1).Should().Be("ffffffff-1111-2222-3333-444444444444");
        r.GetString(3).Should().Be("0.0001");
        r.GetInt64(4).Should().Be(2);     // blob length = 2
        r.GetString(5).Should().Be("");   // empty string preserved (not NULL)
        r.GetInt64(6).Should().Be(0);
        r.GetInt64(7).Should().Be(9223372036854775807L);
    }

    [Fact]
    public void InsertRows_HandlesLargeBlobs()
    {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = """CREATE TABLE "BlobTest" ("Id" INTEGER NOT NULL, "Data" BLOB NULL);""";
            cmd.ExecuteNonQuery();
        }

        var plan = new Core.Planning.TablePlan
        {
            Schema = "dbo",
            Name = "BlobTest",
            Columns =
            [
                new() { Name = "Id", Ordinal = 0, SqlType = "int", IsNullable = false },
                new() { Name = "Data", Ordinal = 1, SqlType = "varbinary", IsNullable = true, MaxLength = -1 },
            ]
        };

        var largeBlob = new byte[100_000];
        new Random(42).NextBytes(largeBlob);

        var rows = new List<object?[]>
        {
            new object?[] { 1L, largeBlob },
            new object?[] { 2L, null },
        };

        CoreAdapters.InsertRows(conn, plan, rows);

        using var q = conn.CreateCommand();
        q.CommandText = "SELECT length(Data) FROM BlobTest WHERE Id = 1;";
        ((long)q.ExecuteScalar()!).Should().Be(100_000);

        q.CommandText = "SELECT Data IS NULL FROM BlobTest WHERE Id = 2;";
        ((long)q.ExecuteScalar()!).Should().Be(1);
    }
}
