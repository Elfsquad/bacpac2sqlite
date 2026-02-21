using BacpacToSqlite.Core.Planning;

namespace BacpacToSqlite.Core;

public sealed class TableMapping
{
    public required TablePlan Table { get; init; }
    public required IReadOnlyList<string> BcpEntryNames { get; init; }
}

public static class TableDataLocator
{
    public static IReadOnlyList<TableMapping> MapBcpEntriesToTables(
        BacpacArchiveReader archive,
        Dictionary<(string Schema, string Table), TablePlan> tables)
    {
        var bcpEntries = archive.FindEntriesBySuffix(".bcp");
        var mappings = new List<TableMapping>();

        // BACPAC stores table data at paths like:
        //   Data/SchemaName.TableName/TableData/000.bcp
        //   Data/SchemaName.TableName/TableData/001.bcp
        // or sometimes:
        //   Data/dbo.MyTable/TableData/Data.bcp

        // Group .bcp entries by their parent directory (table path)
        var grouped = bcpEntries
            .GroupBy(e => GetTableIdentifier(e.FullName))
            .ToDictionary(g => g.Key, g => g.OrderBy(e => e.FullName).ToList());

        foreach (var (tableId, entries) in grouped)
        {
            var (schema, table) = ParseTableFromIdentifier(tableId);
            var key = (schema, table);

            // Try exact match first, then case-insensitive
            if (!tables.TryGetValue(key, out var plan))
            {
                plan = tables.Values.FirstOrDefault(t =>
                    t.Schema.Equals(schema, StringComparison.OrdinalIgnoreCase) &&
                    t.Name.Equals(table, StringComparison.OrdinalIgnoreCase));
            }

            if (plan != null)
            {
                mappings.Add(new TableMapping
                {
                    Table = plan,
                    BcpEntryNames = entries.Select(e => e.FullName).ToList()
                });
            }
        }

        return mappings;
    }

    private static string GetTableIdentifier(string entryName)
    {
        // Real paths: "Data/dbo.MyTable/TableData-001-00000.BCP" → "dbo.MyTable"
        // The table identifier is always the second path segment.
        var parts = entryName.Split('/');
        return parts.Length >= 2 ? parts[1] : entryName;
    }

    private static (string Schema, string Table) ParseTableFromIdentifier(string identifier)
    {
        // identifier is like "dbo.MyTable" or "Sales.Orders"
        var dotIndex = identifier.IndexOf('.');
        if (dotIndex > 0)
        {
            return (identifier[..dotIndex], identifier[(dotIndex + 1)..]);
        }
        return ("dbo", identifier);
    }
}
