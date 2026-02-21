# BacpacToSqlite

A .NET 8 command-line tool that converts Microsoft SQL Server `.bacpac` exports to SQLite databases.

## Installation

Requires [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0). No build step needed — the wrapper script handles it.

## Usage

```bash
./bacpac2sqlite --bacpac <path> --sqlite <path> [options]
```

### Options

| Option | Default | Description |
|---|---|---|
| `--bacpac` (required) | | Path to `.bacpac` file |
| `--sqlite` (required) | | Path to SQLite output (created if missing) |
| `--tables` | all | Table name globs to include |
| `--exclude` | none | Table name globs to exclude |
| `--batchSize` | 5000 | Rows per transaction batch |
| `--pragmaFast` | on | Apply fast PRAGMA settings |
| `--foreignKeysOff` | on | Disable foreign keys during import |
| `--truncate` | off | Truncate existing tables before import |
| `--validateCounts` | on | Validate row counts after import |
| `--bcpProfile` | auto | BCP profile: `auto`, `native`, `unicode-native` |
| `--inspect` | off | Print archive inventory and exit |
| `--verbose` | off | Verbose output |

### Examples

Convert a BACPAC to SQLite:

```bash
./bacpac2sqlite --bacpac database.bacpac --sqlite output.db
```

Convert only specific tables:

```bash
./bacpac2sqlite --bacpac database.bacpac --sqlite output.db \
  --tables "dbo.Orders" "dbo.Customers"
```

Exclude tables by pattern:

```bash
./bacpac2sqlite --bacpac database.bacpac --sqlite output.db \
  --exclude "*Log*" "*Audit*"
```

Inspect BACPAC contents without converting:

```bash
./bacpac2sqlite --bacpac database.bacpac --inspect --verbose
```

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 2 | Invalid arguments or missing input file |
| 3 | BACPAC parsing error |
| 4 | One or more tables failed (partial success) |

## Supported SQL Server types

Integers (`tinyint`, `smallint`, `int`, `bigint`), `bit`, `float`, `real`, `uniqueidentifier`, `decimal`/`numeric`, `money`/`smallmoney`, `date`, `time`, `datetime`, `datetime2`, `datetimeoffset`, `smalldatetime`, `char`/`varchar`/`nvarchar`/`nchar`/`text`/`ntext`, `binary`/`varbinary`/`image`, `xml`, `sql_variant`, `hierarchyid`, `geometry`, `geography`, `timestamp`/`rowversion`.

Computed columns are automatically skipped (they are not stored in BACPAC BCP data).

## Running tests

```bash
dotnet test BacpacToSqlite/
```

## Project structure

```
BacpacToSqlite/
  src/
    BacpacToSqlite.Cli/     Console application (System.CommandLine)
    BacpacToSqlite.Core/     Core library
      BcpNative/             BCP binary format parser
      Planning/              Table/column metadata
  tests/
    BacpacToSqlite.Tests/    xUnit tests
```
