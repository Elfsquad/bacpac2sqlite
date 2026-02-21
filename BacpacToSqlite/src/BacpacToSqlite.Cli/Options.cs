namespace BacpacToSqlite.Cli;

public sealed class Options
{
    public required string BacpacPath { get; init; }
    public required string SqlitePath { get; init; }
    public string[] Tables { get; init; } = [];
    public string[] Exclude { get; init; } = [];
    public int BatchSize { get; init; } = 5000;
    public bool PragmaFast { get; init; } = true;
    public bool ForeignKeysOff { get; init; } = true;
    public bool Truncate { get; init; }
    public bool ValidateCounts { get; init; } = true;
    public string BcpProfile { get; init; } = "auto";
    public bool Inspect { get; init; }
    public bool Verbose { get; init; }
}
