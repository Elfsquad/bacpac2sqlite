namespace BacpacToSqlite.Core.BcpNative;

public sealed class BcpRowReader : IDisposable
{
    private readonly BinaryReader _reader;

    public BcpRowReader(Stream stream)
    {
        _reader = new BinaryReader(stream);
    }

    public object? ReadColumn(ColumnFormat format, int precision, int scale)
    {
        if (format.PrefixLength > 0)
        {
            return ReadPrefixedColumn(format, precision, scale);
        }
        else
        {
            return ReadFixedColumn(format, precision, scale);
        }
    }

    private object? ReadPrefixedColumn(ColumnFormat format, int precision, int scale)
    {
        long length = format.PrefixLength switch
        {
            1 => ReadPrefix1(),
            2 => ReadPrefix2(),
            4 => ReadPrefix4(),
            8 => ReadPrefix8(),
            _ => throw new InvalidOperationException($"Invalid prefix length: {format.PrefixLength}")
        };

        // NULL sentinel: -1 for signed prefix, or max unsigned value
        if (length < 0 || length == GetNullSentinel(format.PrefixLength))
        {
            return null;
        }

        if (length == 0)
        {
            // Empty (not null) - return type-appropriate empty value
            return BcpTypeDecoders.Decode(format.TypeCategory, ReadOnlySpan<byte>.Empty, format.SqlType, precision, scale);
        }

        var data = _reader.ReadBytes((int)length);
        return BcpTypeDecoders.Decode(format.TypeCategory, data, format.SqlType, precision, scale);
    }

    private object? ReadFixedColumn(ColumnFormat format, int precision, int scale)
    {
        var data = _reader.ReadBytes(format.FixedLength);
        if (data.Length < format.FixedLength)
            throw new EndOfStreamException($"Expected {format.FixedLength} bytes for {format.SqlType}, got {data.Length}");

        return BcpTypeDecoders.Decode(format.TypeCategory, data, format.SqlType, precision, scale);
    }

    private long ReadPrefix1()
    {
        byte b = _reader.ReadByte();
        return b == 0xFF ? -1 : b;
    }

    private long ReadPrefix2()
    {
        ushort val = _reader.ReadUInt16();
        return val == 0xFFFF ? -1 : val;
    }

    private long ReadPrefix4()
    {
        uint val = _reader.ReadUInt32();
        return val == 0xFFFFFFFF ? -1 : val;
    }

    private long ReadPrefix8()
    {
        long val = _reader.ReadInt64();
        return val == -1 ? -1 : val;
    }

    private static long GetNullSentinel(int prefixLength)
    {
        return prefixLength switch
        {
            1 => 0xFF,
            2 => 0xFFFF,
            4 => 0xFFFFFFFF,
            8 => -1,
            _ => -1
        };
    }

    public void Dispose()
    {
        _reader.Dispose();
    }
}
