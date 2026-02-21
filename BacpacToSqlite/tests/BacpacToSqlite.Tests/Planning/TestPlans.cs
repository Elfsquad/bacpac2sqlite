using BacpacToSqlite.Core.Planning;

namespace BacpacToSqlite.Tests.Planning;

public static class TestPlans
{
    public static TablePlan KitchenSink() => new()
    {
        Schema = "dbo",
        Name = "KitchenSink",
        Columns =
        [
            new() { Name = "Id", Ordinal = 0, SqlType = "int", IsNullable = false },
            new() { Name = "TenantId", Ordinal = 1, SqlType = "uniqueidentifier", IsNullable = false },
            new() { Name = "CreatedAt", Ordinal = 2, SqlType = "datetime2", IsNullable = false, Scale = 7 },
            new() { Name = "Price", Ordinal = 3, SqlType = "decimal", IsNullable = false, Precision = 18, Scale = 4 },
            new() { Name = "Payload", Ordinal = 4, SqlType = "varbinary", IsNullable = true, MaxLength = -1 },
            new() { Name = "Title", Ordinal = 5, SqlType = "nvarchar", IsNullable = true, MaxLength = 200 },
            new() { Name = "IsActive", Ordinal = 6, SqlType = "bit", IsNullable = false },
            new() { Name = "Big", Ordinal = 7, SqlType = "bigint", IsNullable = false },
        ]
    };

    public static TablePlan PrefixNulls() => new()
    {
        Schema = "dbo",
        Name = "PrefixNulls",
        Columns =
        [
            new() { Name = "K", Ordinal = 0, SqlType = "int", IsNullable = false },
            new() { Name = "VarcharMaybe", Ordinal = 1, SqlType = "varchar", IsNullable = true, MaxLength = 50 },
            new() { Name = "VarbinaryMaybe", Ordinal = 2, SqlType = "varbinary", IsNullable = true, MaxLength = 50 },
        ]
    };
}
