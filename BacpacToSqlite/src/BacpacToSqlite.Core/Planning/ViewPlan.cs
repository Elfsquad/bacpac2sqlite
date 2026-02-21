namespace BacpacToSqlite.Core.Planning;

public sealed class ViewPlan
{
    public required string Schema { get; init; }
    public required string Name { get; init; }
    public required string SelectStatement { get; init; }
}
