using BacpacToSqlite.Core.Planning;

namespace BacpacToSqlite.Core.BcpNative;

public static class BcpFormatInference
{
    public static BcpFormatDescriptor InferOrAutoDetect(
        TablePlan plan,
        BcpProfile? forcedProfile = null)
    {
        // BACPAC files use unicode-native format (bcp -N):
        // - Character data (char/varchar/nchar/nvarchar) is stored as UTF-16LE with 2-byte prefix
        // - Non-character data uses native binary representation
        // If no profile is forced, default to UnicodeNative as that's what BACPAC uses.
        var profile = forcedProfile ?? BcpProfile.UnicodeNative;

        var columns = new List<ColumnFormat>();

        foreach (var col in plan.Columns)
        {
            var format = InferColumnFormat(col, profile);
            columns.Add(format);
        }

        return new BcpFormatDescriptor
        {
            Profile = profile,
            Columns = columns
        };
    }

    private static ColumnFormat InferColumnFormat(ColumnPlan col, BcpProfile profile)
    {
        var sqlType = col.SqlType.ToLowerInvariant();
        bool bacpac = profile == BcpProfile.UnicodeNative;

        // In BACPAC format, nullable fixed-size types need a 1-byte length prefix
        // to encode NULL (0xFF = NULL, otherwise the byte is the data length).
        // NOT NULL fixed-size types are stored without prefix (just raw bytes).
        // GUID and bit always have 1-byte prefix in BACPAC regardless of nullability.
        bool nullableFixed = bacpac && col.IsNullable;

        return sqlType switch
        {
            // Integer types: fixed in standard BCP; nullable gets 1-byte prefix in BACPAC
            "tinyint" => nullableFixed
                ? Prefixed(BcpTypeCategory.FixedInt, 1, sqlType)
                : Fixed(BcpTypeCategory.FixedInt, 1, sqlType),
            "smallint" => nullableFixed
                ? Prefixed(BcpTypeCategory.FixedInt, 1, sqlType)
                : Fixed(BcpTypeCategory.FixedInt, 2, sqlType),
            "int" or "integer" => nullableFixed
                ? Prefixed(BcpTypeCategory.FixedInt, 1, sqlType)
                : Fixed(BcpTypeCategory.FixedInt, 4, sqlType),
            "bigint" => nullableFixed
                ? Prefixed(BcpTypeCategory.FixedInt, 1, sqlType)
                : Fixed(BcpTypeCategory.FixedInt, 8, sqlType),

            // Bit: fixed in standard BCP, always 1-byte prefix in BACPAC
            "bit" => bacpac
                ? Prefixed(BcpTypeCategory.Bit, 1, sqlType)
                : Fixed(BcpTypeCategory.Bit, 1, sqlType),

            // Float types: fixed in standard BCP; nullable gets 1-byte prefix in BACPAC
            "real" => nullableFixed
                ? Prefixed(BcpTypeCategory.Float, 1, sqlType)
                : Fixed(BcpTypeCategory.Float, 4, sqlType),
            "float" => nullableFixed
                ? Prefixed(BcpTypeCategory.Float, 1, sqlType)
                : Fixed(BcpTypeCategory.Float, 8, sqlType),

            // GUID: fixed in standard BCP, always 1-byte prefix in BACPAC
            "uniqueidentifier" => bacpac
                ? Prefixed(BcpTypeCategory.Guid, 1, sqlType)
                : Fixed(BcpTypeCategory.Guid, 16, sqlType),

            // Date: fixed 3 bytes; nullable gets 1-byte prefix in BACPAC
            "date" => nullableFixed
                ? Prefixed(BcpTypeCategory.Date, 1, sqlType)
                : Fixed(BcpTypeCategory.Date, 3, sqlType),

            // Time: 1-byte prefix in standard BCP; fixed in BACPAC (nullable gets prefix)
            "time" => bacpac
                ? (nullableFixed
                    ? Prefixed(BcpTypeCategory.Time, 1, sqlType)
                    : Fixed(BcpTypeCategory.Time, GetTimeBytesForScale(col.Scale), sqlType))
                : Prefixed(BcpTypeCategory.Time, 1, sqlType),

            // DateTime: fixed 8 bytes; nullable gets 1-byte prefix in BACPAC
            "datetime" => nullableFixed
                ? Prefixed(BcpTypeCategory.DateTime, 1, sqlType)
                : Fixed(BcpTypeCategory.DateTime, 8, sqlType),

            // DateTime2: 1-byte prefix in standard BCP; fixed in BACPAC (nullable gets prefix)
            "datetime2" => bacpac
                ? (nullableFixed
                    ? Prefixed(BcpTypeCategory.DateTime2, 1, sqlType)
                    : Fixed(BcpTypeCategory.DateTime2, GetTimeBytesForScale(col.Scale) + 3, sqlType))
                : Prefixed(BcpTypeCategory.DateTime2, 1, sqlType),

            // DateTimeOffset: 1-byte prefix in standard BCP; fixed in BACPAC (nullable gets prefix)
            "datetimeoffset" => bacpac
                ? (nullableFixed
                    ? Prefixed(BcpTypeCategory.DateTimeOffset, 1, sqlType)
                    : Fixed(BcpTypeCategory.DateTimeOffset, GetTimeBytesForScale(col.Scale) + 5, sqlType))
                : Prefixed(BcpTypeCategory.DateTimeOffset, 1, sqlType),

            // SmallDateTime: fixed 4 bytes; nullable gets 1-byte prefix in BACPAC
            "smalldatetime" => nullableFixed
                ? Prefixed(BcpTypeCategory.SmallDateTime, 1, sqlType)
                : Fixed(BcpTypeCategory.SmallDateTime, 4, sqlType),

            // Decimal/numeric: always 1-byte prefix (variable by precision)
            // BACPAC includes a scale byte before sign+value; standard BCP does not
            "decimal" or "numeric" => Prefixed(
                bacpac ? BcpTypeCategory.DecimalWithScale : BcpTypeCategory.Decimal,
                1, sqlType),

            // Money types: fixed; nullable gets 1-byte prefix in BACPAC
            "money" => nullableFixed
                ? Prefixed(BcpTypeCategory.Money, 1, sqlType)
                : Fixed(BcpTypeCategory.Money, 8, sqlType),
            "smallmoney" => nullableFixed
                ? Prefixed(BcpTypeCategory.SmallMoney, 1, sqlType)
                : Fixed(BcpTypeCategory.SmallMoney, 4, sqlType),

            // Binary types: 2-byte prefix
            "binary" => Prefixed(BcpTypeCategory.Binary, 2, sqlType),

            // Varbinary/image: 2-byte or 8-byte (MAX) prefix
            "varbinary" or "image" => Prefixed(BcpTypeCategory.Binary,
                col.MaxLength == -1 ? 8 : 2, sqlType),

            // Character types: prefix depends on MAX and profile
            "varchar" or "char" or "text" => profile switch
            {
                BcpProfile.UnicodeNative => Prefixed(BcpTypeCategory.UnicodeString,
                    col.MaxLength == -1 ? 8 : 2, sqlType),
                _ => Prefixed(BcpTypeCategory.AnsiString,
                    col.MaxLength == -1 ? 8 : 2, sqlType)
            },

            "nvarchar" or "nchar" or "ntext" or "sysname" => Prefixed(
                BcpTypeCategory.UnicodeString, col.MaxLength == -1 ? 8 : 2, sqlType),

            "xml" => Prefixed(BcpTypeCategory.UnicodeString, 8, sqlType),

            "sql_variant" => Prefixed(BcpTypeCategory.Binary, 4, sqlType),

            "hierarchyid" or "geometry" or "geography" => Prefixed(
                BcpTypeCategory.Binary, 2, sqlType),

            "timestamp" or "rowversion" => Fixed(BcpTypeCategory.Binary, 8, sqlType),

            _ => throw new NotSupportedException($"Unsupported SQL type: {sqlType}")
        };
    }

    private static ColumnFormat Fixed(BcpTypeCategory category, int fixedLength, string sqlType) =>
        new() { TypeCategory = category, PrefixLength = 0, FixedLength = fixedLength, SqlType = sqlType };

    private static ColumnFormat Prefixed(BcpTypeCategory category, int prefixLength, string sqlType) =>
        new() { TypeCategory = category, PrefixLength = prefixLength, FixedLength = 0, SqlType = sqlType };

    private static int GetTimeBytesForScale(int scale)
    {
        int effectiveScale = scale <= 0 ? 7 : scale;
        return effectiveScale switch
        {
            <= 2 => 3,
            <= 4 => 4,
            _ => 5
        };
    }
}
