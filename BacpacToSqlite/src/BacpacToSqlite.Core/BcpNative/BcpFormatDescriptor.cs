namespace BacpacToSqlite.Core.BcpNative;

public enum BcpProfile
{
    Native,        // bcp -n
    UnicodeNative  // bcp -N
}

public enum BcpTypeCategory
{
    FixedInt,      // tinyint, smallint, int, bigint
    Bit,
    Float,         // real, float
    Guid,          // uniqueidentifier
    Binary,        // binary, varbinary
    AnsiString,    // varchar, char
    UnicodeString, // nvarchar, nchar
    Date,
    Time,
    DateTime2,
    DateTime,
    DateTimeOffset,
    Decimal,
    DecimalWithScale, // BACPAC variant: scale byte precedes sign+value
    Money,
    SmallMoney,
    SmallDateTime,
}

public sealed class ColumnFormat
{
    public required BcpTypeCategory TypeCategory { get; init; }
    public required int PrefixLength { get; init; }
    public required int FixedLength { get; init; }
    public required string SqlType { get; init; }
}

public sealed class BcpFormatDescriptor
{
    public required BcpProfile Profile { get; init; }
    public required IReadOnlyList<ColumnFormat> Columns { get; init; }
}
