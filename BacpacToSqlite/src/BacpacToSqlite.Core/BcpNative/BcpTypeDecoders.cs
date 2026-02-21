using System.Buffers.Binary;
using System.Globalization;
using System.Text;

namespace BacpacToSqlite.Core.BcpNative;

public static class BcpTypeDecoders
{
    public static object? Decode(BcpTypeCategory category, ReadOnlySpan<byte> data, string sqlType, int precision, int scale)
    {
        if (data.IsEmpty)
        {
            return category switch
            {
                BcpTypeCategory.AnsiString or BcpTypeCategory.UnicodeString => "",
                BcpTypeCategory.Binary => Array.Empty<byte>(),
                _ => DBNull.Value
            };
        }

        return category switch
        {
            BcpTypeCategory.FixedInt => DecodeInt(data, sqlType),
            BcpTypeCategory.Bit => DecodeBit(data),
            BcpTypeCategory.Float => DecodeFloat(data, sqlType),
            BcpTypeCategory.Guid => DecodeGuid(data),
            BcpTypeCategory.Binary => DecodeBinary(data),
            BcpTypeCategory.AnsiString => DecodeAnsiString(data),
            BcpTypeCategory.UnicodeString => DecodeUnicodeString(data),
            BcpTypeCategory.Date => DecodeDate(data),
            BcpTypeCategory.Time => DecodeTime(data, scale),
            BcpTypeCategory.DateTime2 => DecodeDateTime2(data, scale),
            BcpTypeCategory.DateTime => DecodeDateTime(data),
            BcpTypeCategory.DateTimeOffset => DecodeDateTimeOffset(data, scale),
            BcpTypeCategory.SmallDateTime => DecodeSmallDateTime(data),
            BcpTypeCategory.Decimal => DecodeDecimal(data, precision, scale),
            BcpTypeCategory.DecimalWithScale => DecodeDecimalWithScale(data, precision, scale),
            BcpTypeCategory.Money => DecodeMoney(data),
            BcpTypeCategory.SmallMoney => DecodeSmallMoney(data),
            _ => throw new NotSupportedException($"Unsupported type category: {category}")
        };
    }

    private static object DecodeInt(ReadOnlySpan<byte> data, string sqlType)
    {
        return sqlType switch
        {
            "tinyint" => (long)data[0],
            "smallint" => (long)BinaryPrimitives.ReadInt16LittleEndian(data),
            "int" => (long)BinaryPrimitives.ReadInt32LittleEndian(data),
            "bigint" => BinaryPrimitives.ReadInt64LittleEndian(data),
            _ => (long)BinaryPrimitives.ReadInt32LittleEndian(data)
        };
    }

    private static object DecodeBit(ReadOnlySpan<byte> data)
    {
        return (long)(data[0] != 0 ? 1 : 0);
    }

    private static object DecodeFloat(ReadOnlySpan<byte> data, string sqlType)
    {
        return sqlType == "real"
            ? (double)BinaryPrimitives.ReadSingleLittleEndian(data)
            : BinaryPrimitives.ReadDoubleLittleEndian(data);
    }

    private static object DecodeGuid(ReadOnlySpan<byte> data)
    {
        return new Guid(data).ToString("D");
    }

    private static object DecodeBinary(ReadOnlySpan<byte> data)
    {
        return data.ToArray();
    }

    private static object DecodeAnsiString(ReadOnlySpan<byte> data)
    {
        return Encoding.ASCII.GetString(data);
    }

    private static object DecodeUnicodeString(ReadOnlySpan<byte> data)
    {
        return Encoding.Unicode.GetString(data);
    }

    private static object DecodeDate(ReadOnlySpan<byte> data)
    {
        // 3 bytes: days since 0001-01-01
        int days = data[0] | (data[1] << 8) | (data[2] << 16);
        var date = new DateTime(1, 1, 1).AddDays(days);
        return date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    }

    private static object DecodeTime(ReadOnlySpan<byte> data, int scale)
    {
        var ticks = ReadTimeValue(data, scale);
        var ts = TimeSpan.FromTicks(ticks);
        return ts.ToString(@"hh\:mm\:ss\.fffffff", CultureInfo.InvariantCulture);
    }

    private static object DecodeDateTime2(ReadOnlySpan<byte> data, int scale)
    {
        // datetime2: time component followed by 3-byte date
        int timeBytes = GetTimeBytesForScale(scale);
        var timeTicks = ReadTimeValue(data[..timeBytes], scale);
        int days = data[timeBytes] | (data[timeBytes + 1] << 8) | (data[timeBytes + 2] << 16);

        var date = new DateTime(1, 1, 1).AddDays(days);
        var time = TimeSpan.FromTicks(timeTicks);
        var dt = date.Add(time);
        return dt.ToString("O", CultureInfo.InvariantCulture);
    }

    private static object DecodeDateTime(ReadOnlySpan<byte> data)
    {
        // datetime: 4 bytes days since 1900-01-01, 4 bytes 1/300 second ticks
        int days = BinaryPrimitives.ReadInt32LittleEndian(data);
        int ticks = BinaryPrimitives.ReadInt32LittleEndian(data[4..]);
        var date = new DateTime(1900, 1, 1).AddDays(days);
        var time = TimeSpan.FromMilliseconds(ticks * 10.0 / 3.0);
        return date.Add(time).ToString("O", CultureInfo.InvariantCulture);
    }

    private static object DecodeDateTimeOffset(ReadOnlySpan<byte> data, int scale)
    {
        int timeBytes = GetTimeBytesForScale(scale);
        var timeTicks = ReadTimeValue(data[..timeBytes], scale);
        int dateOffset = timeBytes;
        int days = data[dateOffset] | (data[dateOffset + 1] << 8) | (data[dateOffset + 2] << 16);
        short offsetMinutes = BinaryPrimitives.ReadInt16LittleEndian(data[(dateOffset + 3)..]);

        var date = new DateTime(1, 1, 1).AddDays(days);
        var time = TimeSpan.FromTicks(timeTicks);
        // BCP stores UTC time; convert to local by adding the offset
        var dt = date.Add(time).AddMinutes(offsetMinutes);

        // Format offset as ±HH:mm, avoiding DateTimeOffset constructor validation
        var sign = offsetMinutes >= 0 ? "+" : "-";
        int absMinutes = Math.Abs(offsetMinutes);
        int oh = absMinutes / 60;
        int om = absMinutes % 60;
        return dt.ToString("O", CultureInfo.InvariantCulture).TrimEnd('Z')
            + $"{sign}{oh:D2}:{om:D2}";
    }

    private static object DecodeSmallDateTime(ReadOnlySpan<byte> data)
    {
        // 2 bytes days since 1900-01-01, 2 bytes minutes since midnight
        ushort days = BinaryPrimitives.ReadUInt16LittleEndian(data);
        ushort minutes = BinaryPrimitives.ReadUInt16LittleEndian(data[2..]);
        var date = new DateTime(1900, 1, 1).AddDays(days).AddMinutes(minutes);
        return date.ToString("O", CultureInfo.InvariantCulture);
    }

    private static object DecodeDecimalWithScale(ReadOnlySpan<byte> data, int precision, int scale)
    {
        // BACPAC BCP decimal: 1 byte precision, 1 byte scale, 1 byte sign (1=pos, 0=neg), then value bytes LE
        if (data.Length < 4) return "0";
        // First two bytes are precision and scale (redundant with column metadata, skip them)
        return DecodeDecimalCore(data[2..], precision, scale);
    }

    private static object DecodeDecimal(ReadOnlySpan<byte> data, int precision, int scale)
    {
        // Standard BCP decimal: 1 byte sign (1=pos, 0=neg), then value bytes LE
        if (data.Length < 2) return "0";
        return DecodeDecimalCore(data, precision, scale);
    }

    private static object DecodeDecimalCore(ReadOnlySpan<byte> data, int precision, int scale)
    {
        byte sign = data[0];
        var valueBytes = data[1..];

        // Read as a big integer (little-endian unsigned)
        var magnitude = System.Numerics.BigInteger.Zero;
        for (int i = valueBytes.Length - 1; i >= 0; i--)
        {
            magnitude = (magnitude << 8) | valueBytes[i];
        }

        if (sign == 0) magnitude = -magnitude;

        // Apply scale
        if (scale > 0)
        {
            var divisor = System.Numerics.BigInteger.Pow(10, scale);
            var intPart = System.Numerics.BigInteger.DivRem(
                System.Numerics.BigInteger.Abs(magnitude), divisor, out var remainder);
            var negSign = magnitude < 0 ? "-" : "";
            return $"{negSign}{intPart}.{System.Numerics.BigInteger.Abs(remainder).ToString().PadLeft(scale, '0')}";
        }

        return magnitude.ToString();
    }

    private static object DecodeMoney(ReadOnlySpan<byte> data)
    {
        // money: 8 bytes, stored as hi4:lo4, value = (hi<<32|lo) / 10000
        int hi = BinaryPrimitives.ReadInt32LittleEndian(data);
        uint lo = BinaryPrimitives.ReadUInt32LittleEndian(data[4..]);
        long val = ((long)hi << 32) | lo;
        return (val / 10000m).ToString(CultureInfo.InvariantCulture);
    }

    private static object DecodeSmallMoney(ReadOnlySpan<byte> data)
    {
        int val = BinaryPrimitives.ReadInt32LittleEndian(data);
        return (val / 10000m).ToString(CultureInfo.InvariantCulture);
    }

    private static long ReadTimeValue(ReadOnlySpan<byte> data, int scale)
    {
        long raw = 0;
        for (int i = data.Length - 1; i >= 0; i--)
        {
            raw = (raw << 8) | data[i];
        }

        // Convert from scale units to 100ns ticks
        int effectiveScale = scale <= 0 ? 7 : scale;
        long ticksPerUnit = effectiveScale switch
        {
            0 => 10_000_000L,
            1 => 1_000_000L,
            2 => 100_000L,
            3 => 10_000L,
            4 => 1_000L,
            5 => 100L,
            6 => 10L,
            7 => 1L,
            _ => 1L
        };

        return raw * ticksPerUnit;
    }

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
