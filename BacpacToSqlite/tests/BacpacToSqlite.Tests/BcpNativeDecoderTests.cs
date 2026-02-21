using System.Text;
using BacpacToSqlite.Core.BcpNative;
using BacpacToSqlite.Core.Planning;

namespace BacpacToSqlite.Tests;

public class BcpNativeDecoderTests
{
    [Fact]
    public void DecodeFixedIntTypes()
    {
        // Encode: tinyint=42, smallint=1000, int=100000, bigint=9876543210
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((byte)42);                      // tinyint
        bw.Write((short)1000);                    // smallint
        bw.Write(100000);                         // int
        bw.Write(9876543210L);                    // bigint
        bw.Flush();
        ms.Position = 0;

        var plan = MakePlan(
            Col("TinyVal", "tinyint"),
            Col("SmallVal", "smallint"),
            Col("IntVal", "int"),
            Col("BigVal", "bigint"));

        var format = BcpFormatInference.InferOrAutoDetect(plan, BcpProfile.Native);
        var rows = BcpStreamDecoder.DecodeRows([ms], plan, format).ToList();

        Assert.Single(rows);
        Assert.Equal(42L, rows[0][0]);
        Assert.Equal(1000L, rows[0][1]);
        Assert.Equal(100000L, rows[0][2]);
        Assert.Equal(9876543210L, rows[0][3]);
    }

    [Fact]
    public void DecodeBitType()
    {
        var ms = new MemoryStream([1, 0]);
        var plan = MakePlan(Col("B1", "bit"), Col("B2", "bit"));
        var format = BcpFormatInference.InferOrAutoDetect(plan, BcpProfile.Native);
        var rows = BcpStreamDecoder.DecodeRows([ms], plan, format).ToList();

        Assert.Single(rows);
        Assert.Equal(1L, rows[0][0]);
        Assert.Equal(0L, rows[0][1]);
    }

    [Fact]
    public void DecodeFloatTypes()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write(3.14f);     // real
        bw.Write(2.718281828); // float
        bw.Flush();
        ms.Position = 0;

        var plan = MakePlan(Col("R", "real"), Col("F", "float"));
        var format = BcpFormatInference.InferOrAutoDetect(plan, BcpProfile.Native);
        var rows = BcpStreamDecoder.DecodeRows([ms], plan, format).ToList();

        Assert.Single(rows);
        Assert.Equal(3.14, (double)rows[0][0]!, 2);
        Assert.Equal(2.718281828, (double)rows[0][1]!, 6);
    }

    [Fact]
    public void DecodeGuid()
    {
        var guid = Guid.Parse("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        var ms = new MemoryStream(guid.ToByteArray());

        var plan = MakePlan(Col("G", "uniqueidentifier"));
        var format = BcpFormatInference.InferOrAutoDetect(plan, BcpProfile.Native);
        var rows = BcpStreamDecoder.DecodeRows([ms], plan, format).ToList();

        Assert.Single(rows);
        Assert.Equal(guid.ToString("D"), rows[0][0]);
    }

    [Fact]
    public void DecodeUnicodeString_WithPrefix()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        var text = "hello";
        var bytes = Encoding.Unicode.GetBytes(text);
        bw.Write((ushort)bytes.Length); // 2-byte prefix
        bw.Write(bytes);
        bw.Flush();
        ms.Position = 0;

        var plan = MakePlan(Col("S", "nvarchar", maxLength: 100));
        var format = BcpFormatInference.InferOrAutoDetect(plan, BcpProfile.UnicodeNative);
        var rows = BcpStreamDecoder.DecodeRows([ms], plan, format).ToList();

        Assert.Single(rows);
        Assert.Equal("hello", rows[0][0]);
    }

    [Fact]
    public void DecodeNullValue_PrefixMinus1()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((ushort)0xFFFF); // null sentinel for 2-byte prefix
        bw.Flush();
        ms.Position = 0;

        var plan = MakePlan(Col("S", "nvarchar", maxLength: 100));
        var format = BcpFormatInference.InferOrAutoDetect(plan, BcpProfile.UnicodeNative);
        var rows = BcpStreamDecoder.DecodeRows([ms], plan, format).ToList();

        Assert.Single(rows);
        Assert.Null(rows[0][0]);
    }

    [Fact]
    public void DecodeEmptyString_PrefixZero()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write((ushort)0); // zero-length prefix
        bw.Flush();
        ms.Position = 0;

        var plan = MakePlan(Col("S", "nvarchar", maxLength: 100));
        var format = BcpFormatInference.InferOrAutoDetect(plan, BcpProfile.UnicodeNative);
        var rows = BcpStreamDecoder.DecodeRows([ms], plan, format).ToList();

        Assert.Single(rows);
        // Empty span decoded as empty string, not null
        Assert.NotNull(rows[0][0]);
    }

    [Fact]
    public void DecodeVarbinary_WithPrefix()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        byte[] payload = [0xDE, 0xAD, 0xBE, 0xEF];
        bw.Write((ushort)payload.Length);
        bw.Write(payload);
        bw.Flush();
        ms.Position = 0;

        var plan = MakePlan(Col("B", "varbinary", maxLength: 100));
        var format = BcpFormatInference.InferOrAutoDetect(plan, BcpProfile.Native);
        var rows = BcpStreamDecoder.DecodeRows([ms], plan, format).ToList();

        Assert.Single(rows);
        Assert.Equal(payload, (byte[])rows[0][0]!);
    }

    [Fact]
    public void DecodeDate()
    {
        // 2024-06-15 = days since 0001-01-01
        var date = new DateTime(2024, 6, 15);
        int days = (int)(date - new DateTime(1, 1, 1)).TotalDays;
        var ms = new MemoryStream([
            (byte)(days & 0xFF),
            (byte)((days >> 8) & 0xFF),
            (byte)((days >> 16) & 0xFF)
        ]);

        var plan = MakePlan(Col("D", "date"));
        var format = BcpFormatInference.InferOrAutoDetect(plan, BcpProfile.Native);
        var rows = BcpStreamDecoder.DecodeRows([ms], plan, format).ToList();

        Assert.Single(rows);
        Assert.Equal("2024-06-15", rows[0][0]);
    }

    [Fact]
    public void DecodeMultipleRows()
    {
        var ms = new MemoryStream();
        var bw = new BinaryWriter(ms);
        bw.Write(1);
        bw.Write(2);
        bw.Write(3);
        bw.Flush();
        ms.Position = 0;

        var plan = MakePlan(Col("V", "int"));
        var format = BcpFormatInference.InferOrAutoDetect(plan, BcpProfile.Native);
        var rows = BcpStreamDecoder.DecodeRows([ms], plan, format).ToList();

        Assert.Equal(3, rows.Count);
        Assert.Equal(1L, rows[0][0]);
        Assert.Equal(2L, rows[1][0]);
        Assert.Equal(3L, rows[2][0]);
    }

    [Fact]
    public void DecodeMultipleParts()
    {
        var ms1 = new MemoryStream();
        var bw1 = new BinaryWriter(ms1);
        bw1.Write(10);
        bw1.Flush();
        ms1.Position = 0;

        var ms2 = new MemoryStream();
        var bw2 = new BinaryWriter(ms2);
        bw2.Write(20);
        bw2.Flush();
        ms2.Position = 0;

        var plan = MakePlan(Col("V", "int"));
        var format = BcpFormatInference.InferOrAutoDetect(plan, BcpProfile.Native);
        var rows = BcpStreamDecoder.DecodeRows([ms1, ms2], plan, format).ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal(10L, rows[0][0]);
        Assert.Equal(20L, rows[1][0]);
    }

    private static TablePlan MakePlan(params ColumnPlan[] columns)
    {
        return new TablePlan
        {
            Schema = "dbo",
            Name = "Test",
            Columns = columns.ToList()
        };
    }

    private static ColumnPlan Col(string name, string sqlType, bool nullable = true, int maxLength = -1)
    {
        return new ColumnPlan
        {
            Name = name,
            Ordinal = 0,
            SqlType = sqlType,
            IsNullable = nullable,
            MaxLength = maxLength,
            Precision = sqlType is "decimal" or "numeric" ? 18 : 0,
            Scale = sqlType is "decimal" or "numeric" ? 4 : (sqlType is "datetime2" or "time" ? 7 : 0)
        };
    }
}
