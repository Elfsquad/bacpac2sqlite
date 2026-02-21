namespace BacpacToSqlite.Core.Planning;

public sealed class ColumnPlan
{
    public required string Name { get; init; }
    public required int Ordinal { get; init; }
    public required string SqlType { get; init; }
    public required bool IsNullable { get; init; }
    public int MaxLength { get; init; } = -1;
    public int Precision { get; init; }
    public int Scale { get; init; }
}
