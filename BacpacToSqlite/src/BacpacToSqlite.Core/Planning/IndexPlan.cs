namespace BacpacToSqlite.Core.Planning;

public sealed class IndexPlan
{
    public required string Name { get; init; }
    public required string Schema { get; init; }
    public required string TableName { get; init; }
    public required IReadOnlyList<string> ColumnNames { get; init; }
    public required bool IsUnique { get; init; }
}
