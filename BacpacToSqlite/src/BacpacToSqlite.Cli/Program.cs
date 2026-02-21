using System.CommandLine;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using BacpacToSqlite.Cli;
using BacpacToSqlite.Core;
using BacpacToSqlite.Core.BcpNative;
using BacpacToSqlite.Core.Planning;
using BacpacToSqlite.Core.Sqlite;

var bacpacOption = new Option<FileInfo>("--bacpac") { Description = "Path to .bacpac file", Required = true };
var sqliteOption = new Option<FileInfo>("--sqlite") { Description = "Path to SQLite database (created if it doesn't exist)", Required = true };
var tablesOption = new Option<string[]>("--tables") { Description = "Table name globs to include", AllowMultipleArgumentsPerToken = true };
var excludeOption = new Option<string[]>("--exclude") { Description = "Table name globs to exclude", AllowMultipleArgumentsPerToken = true };
var batchSizeOption = new Option<int>("--batchSize") { Description = "Rows per transaction batch", DefaultValueFactory = _ => 5000 };
var pragmaFastOption = new Option<bool>("--pragmaFast") { Description = "Apply fast PRAGMA settings", DefaultValueFactory = _ => true };
var foreignKeysOffOption = new Option<bool>("--foreignKeysOff") { Description = "Disable foreign keys during import", DefaultValueFactory = _ => true };
var truncateOption = new Option<bool>("--truncate") { Description = "Truncate tables before import" };
var validateCountsOption = new Option<bool>("--validateCounts") { Description = "Validate row counts after import", DefaultValueFactory = _ => true };
var bcpProfileOption = new Option<string>("--bcpProfile") { Description = "BCP profile: auto, native, unicode-native", DefaultValueFactory = _ => "auto" };
var inspectOption = new Option<bool>("--inspect") { Description = "Print archive inventory and exit" };
var verboseOption = new Option<bool>("--verbose") { Description = "Verbose output" };

var rootCommand = new RootCommand("Convert BACPAC to SQLite")
{
    bacpacOption, sqliteOption, tablesOption, excludeOption,
    batchSizeOption, pragmaFastOption, foreignKeysOffOption, truncateOption,
    validateCountsOption, bcpProfileOption, inspectOption, verboseOption
};

rootCommand.SetAction(parseResult =>
{
    var options = new Options
    {
        BacpacPath = parseResult.GetValue(bacpacOption)!.FullName,
        SqlitePath = parseResult.GetValue(sqliteOption)!.FullName,
        Tables = parseResult.GetValue(tablesOption) ?? [],
        Exclude = parseResult.GetValue(excludeOption) ?? [],
        BatchSize = parseResult.GetValue(batchSizeOption),
        PragmaFast = parseResult.GetValue(pragmaFastOption),
        ForeignKeysOff = parseResult.GetValue(foreignKeysOffOption),
        Truncate = parseResult.GetValue(truncateOption),
        ValidateCounts = parseResult.GetValue(validateCountsOption),
        BcpProfile = parseResult.GetValue(bcpProfileOption)!,
        Inspect = parseResult.GetValue(inspectOption),
        Verbose = parseResult.GetValue(verboseOption)
    };

    return Run(options);
});

return rootCommand.Parse(args).Invoke();

static int Run(Options options)
{
    if (!File.Exists(options.BacpacPath))
    {
        Console.Error.WriteLine($"Error: BACPAC file not found: {options.BacpacPath}");
        return 2;
    }

    if (!options.Inspect && string.IsNullOrEmpty(options.SqlitePath))
    {
        Console.Error.WriteLine("Error: --sqlite path is required for conversion");
        return 2;
    }

    try
    {
        using var archive = new BacpacArchiveReader(options.BacpacPath);

        // Parse model.xml
        var modelEntry = archive.FindEntryByName("model.xml")
            ?? throw new InvalidOperationException("model.xml not found in BACPAC");

        ModelParseResult model;
        using (var modelStream = archive.OpenEntry(modelEntry.FullName))
        {
            model = ModelXmlParser.Parse(modelStream);
        }

        if (options.Verbose)
        {
            Console.WriteLine($"Parsed {model.Tables.Count} tables, {model.Indices.Count} indices, {model.Views.Count} views from model.xml");
        }

        // Locate BCP data
        var mappings = TableDataLocator.MapBcpEntriesToTables(archive, model.Tables);

        if (options.Verbose)
            Console.WriteLine($"Found BCP data for {mappings.Count} tables");

        // Inspect mode
        if (options.Inspect)
        {
            RunInspect(archive, model, mappings, options);
            return 0;
        }

        // Filter tables
        var filteredMappings = FilterMappings(mappings, options.Tables, options.Exclude);

        if (options.Verbose)
            Console.WriteLine($"Processing {filteredMappings.Count} tables after filtering");

        // Determine BCP profile
        BcpProfile? forcedProfile = options.BcpProfile.ToLowerInvariant() switch
        {
            "native" => BcpProfile.Native,
            "unicode-native" => BcpProfile.UnicodeNative,
            _ => null
        };

        // Open SQLite connection (creates file if it doesn't exist)
        var connStr = new SqliteConnectionStringBuilder { DataSource = options.SqlitePath }.ToString();
        using var connection = new SqliteConnection(connStr);
        connection.Open();

        // Create schema from model.xml metadata
        SqliteSchemaCreator.CreateTables(connection, filteredMappings.Select(m => m.Table));

        if (options.Verbose)
            Console.WriteLine($"Created {filteredMappings.Count} tables in SQLite");

        // Set up bulk inserter
        using var inserter = new SqliteBulkInserter(
            connection,
            options.PragmaFast,
            options.ForeignKeysOff,
            options.Truncate,
            options.BatchSize);

        inserter.ApplyPragmas();

        // Process each table
        int successCount = 0;
        int errorCount = 0;

        foreach (var mapping in filteredMappings)
        {
            var table = mapping.Table;
            Console.Write($"  {table.Schema}.{table.Name}...");

            try
            {
                var format = BcpFormatInference.InferOrAutoDetect(table, forcedProfile);

                var bcpStreams = mapping.BcpEntryNames
                    .Select(name => archive.OpenEntry(name));

                var rows = BcpStreamDecoder.DecodeRows(bcpStreams, table, format);
                var rowCount = inserter.InsertRows(table, rows);

                Console.WriteLine($" {rowCount:N0} rows");
                successCount++;

                // Validate
                if (options.ValidateCounts)
                {
                    var sqliteCount = inserter.GetRowCount(table.Name);
                    if (sqliteCount < rowCount)
                    {
                        Console.Error.WriteLine(
                            $"  WARNING: {table.Name} - decoded {rowCount} rows but SQLite has {sqliteCount}");
                    }
                }
            }
            catch (Exception ex) when (ex is NotSupportedException or EndOfStreamException
                or ArgumentOutOfRangeException or ArgumentException or OverflowException
                or InvalidOperationException or SqliteException)
            {
                errorCount++;
                Console.Error.WriteLine($" ERROR: {ex.Message}");
                if (options.Verbose)
                    Console.Error.WriteLine($"    {ex.GetType().Name}: {ex.StackTrace?.Split('\n').FirstOrDefault()?.Trim()}");
            }
        }

        // Create indices (after data import for better performance)
        var includedTables = filteredMappings.Select(m => m.Table.Name).ToHashSet();
        var indexCount = SqliteSchemaCreator.CreateIndices(connection, model.Indices, includedTables);
        if (indexCount > 0 || options.Verbose)
            Console.WriteLine($"  Created {indexCount} indices");

        // Create views
        if (model.Views.Count > 0)
        {
            var (viewsCreated, viewsFailed) = SqliteSchemaCreator.CreateViews(connection, model.Views);
            if (viewsCreated > 0 || viewsFailed > 0 || options.Verbose)
            {
                Console.Write($"  Created {viewsCreated} views");
                if (viewsFailed > 0)
                    Console.Write($" ({viewsFailed} skipped due to SQL Server-specific syntax)");
                Console.WriteLine();
            }
        }

        inserter.RestoreForeignKeys();

        Console.WriteLine();
        Console.WriteLine($"Done: {successCount} tables imported, {errorCount} errors.");
        return errorCount > 0 ? 4 : 0;
    }
    catch (InvalidOperationException ex) when (ex.Message.Contains("model.xml") || ex.Message.Contains("archive"))
    {
        Console.Error.WriteLine($"BACPAC parse error: {ex.Message}");
        return 3;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Error: {ex.Message}");
        if (options.Verbose)
            Console.Error.WriteLine(ex.StackTrace);
        return 3;
    }
}

static void RunInspect(
    BacpacArchiveReader archive,
    ModelParseResult model,
    IReadOnlyList<TableMapping> mappings,
    Options options)
{
    var entries = archive.ListEntries();
    var bcpEntries = archive.FindEntriesBySuffix(".bcp");

    Console.WriteLine("=== BACPAC Archive Inventory ===");
    Console.WriteLine($"Total entries: {entries.Count}");
    Console.WriteLine($"BCP data entries: {bcpEntries.Count}");
    Console.WriteLine();

    Console.WriteLine("=== Schema Summary ===");
    foreach (var (key, table) in model.Tables.OrderBy(t => t.Key.Schema).ThenBy(t => t.Key.Table))
    {
        Console.WriteLine($"  {table.Schema}.{table.Name} ({table.Columns.Count} columns)");
        if (options.Verbose)
        {
            foreach (var col in table.Columns)
            {
                var nullable = col.IsNullable ? "NULL" : "NOT NULL";
                Console.WriteLine($"    [{col.Ordinal}] {col.Name} {col.SqlType} {nullable}");
            }
        }
    }
    Console.WriteLine();

    // Show indices
    if (model.Indices.Count > 0)
    {
        Console.WriteLine($"=== Indices ({model.Indices.Count}) ===");
        foreach (var index in model.Indices.OrderBy(i => i.TableName).ThenBy(i => i.Name))
        {
            var unique = index.IsUnique ? "UNIQUE " : "";
            var cols = string.Join(", ", index.ColumnNames);
            Console.WriteLine($"  {unique}{index.Name} ON {index.Schema}.{index.TableName} ({cols})");
        }
        Console.WriteLine();
    }

    // Show views
    if (model.Views.Count > 0)
    {
        Console.WriteLine($"=== Views ({model.Views.Count}) ===");
        foreach (var view in model.Views.OrderBy(v => v.Schema).ThenBy(v => v.Name))
        {
            Console.WriteLine($"  {view.Schema}.{view.Name}");
            if (options.Verbose)
            {
                var preview = view.SelectStatement.Length > 200
                    ? view.SelectStatement[..200] + "..."
                    : view.SelectStatement;
                Console.WriteLine($"    {preview}");
            }
        }
        Console.WriteLine();
    }

    Console.WriteLine("=== Table -> BCP Mapping ===");
    foreach (var mapping in mappings.OrderBy(m => m.Table.Schema).ThenBy(m => m.Table.Name))
    {
        Console.WriteLine($"  {mapping.Table.Schema}.{mapping.Table.Name}:");
        foreach (var entry in mapping.BcpEntryNames)
        {
            var info = archive.FindEntriesBySuffix(".bcp")
                .FirstOrDefault(e => e.FullName == entry);
            var size = info != null ? $" ({info.Length:N0} bytes)" : "";
            Console.WriteLine($"    {entry}{size}");
        }

        if (options.Verbose && mapping.BcpEntryNames.Count > 0)
        {
            var firstBytes = archive.ReadEntryBytes(mapping.BcpEntryNames[0], 64);
            Console.Write("    First 64 bytes: ");
            Console.WriteLine(BitConverter.ToString(firstBytes).Replace("-", " "));
        }
    }

    // Show unmapped BCP entries
    var mappedEntries = mappings.SelectMany(m => m.BcpEntryNames).ToHashSet();
    var unmapped = bcpEntries.Where(e => !mappedEntries.Contains(e.FullName)).ToList();
    if (unmapped.Count > 0)
    {
        Console.WriteLine();
        Console.WriteLine("=== Unmapped BCP Entries ===");
        foreach (var entry in unmapped)
        {
            Console.WriteLine($"  {entry.FullName} ({entry.Length:N0} bytes)");
        }
    }
}

static List<TableMapping> FilterMappings(
    IReadOnlyList<TableMapping> mappings,
    string[] includes,
    string[] excludes)
{
    var result = mappings.ToList();

    if (includes.Length > 0)
    {
        result = result.Where(m =>
            includes.Any(pattern => GlobMatch(
                $"{m.Table.Schema}.{m.Table.Name}", pattern))).ToList();
    }

    if (excludes.Length > 0)
    {
        result = result.Where(m =>
            !excludes.Any(pattern => GlobMatch(
                $"{m.Table.Schema}.{m.Table.Name}", pattern))).ToList();
    }

    return result;
}

static bool GlobMatch(string input, string pattern)
{
    var regexPattern = "^" + Regex.Escape(pattern)
        .Replace("\\*", ".*")
        .Replace("\\?", ".") + "$";
    return Regex.IsMatch(input, regexPattern, RegexOptions.IgnoreCase);
}
