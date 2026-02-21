namespace BacpacToSqlite.Core.Planning;

public sealed class ModelParseResult
{
    public required Dictionary<(string Schema, string Table), TablePlan> Tables { get; init; }
    public required IReadOnlyList<IndexPlan> Indices { get; init; }
    public required IReadOnlyList<ViewPlan> Views { get; init; }
}
