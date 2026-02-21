using BacpacToSqlite.Core.BcpNative;
using BacpacToSqlite.Core.Planning;
using BacpacToSqlite.Core.Sqlite;
using Microsoft.Data.Sqlite;

namespace BacpacToSqlite.Tests;

public enum BcpProfileOption
{
    Auto = 0,
    Native = 1,
    UnicodeNative = 2
}

internal static class CoreAdapters
{
    public static BcpFormatDescriptor InferFormat(Stream bcpPartStream, TablePlan plan, BcpProfileOption option)
    {
        BcpProfile? forced = option switch
        {
            BcpProfileOption.Native => BcpProfile.Native,
            BcpProfileOption.UnicodeNative => BcpProfile.UnicodeNative,
            _ => null
        };

        return BcpFormatInference.InferOrAutoDetect(plan, forced);
    }

    public static IEnumerable<object?[]> DecodeRows(
        IEnumerable<Stream> bcpPartStreams,
        TablePlan plan,
        BcpFormatDescriptor format)
    {
        return BcpStreamDecoder.DecodeRows(bcpPartStreams, plan, format);
    }

    public static long InsertRows(SqliteConnection conn, TablePlan plan, IEnumerable<object?[]> rows)
    {
        using var inserter = new SqliteBulkInserter(conn, pragmaFast: false, foreignKeysOff: false);
        return inserter.InsertRows(plan, rows);
    }
}
