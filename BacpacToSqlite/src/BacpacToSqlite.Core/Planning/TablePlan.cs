namespace BacpacToSqlite.Core.Planning;

public sealed class TablePlan
{
    public required string Schema { get; init; }
    public required string Name { get; init; }
    public required IReadOnlyList<ColumnPlan> Columns { get; init; }

    public string FullName => $"[{Schema}].[{Name}]";
}
