using BacpacToSqlite.Tests.Fixtures;
using BacpacToSqlite.Tests.Planning;
using FluentAssertions;

namespace BacpacToSqlite.Tests.BcpNative;

public class TypeEdgeCaseTests
{
    [Fact]
    public void Decode_KitchenSink_Row1_ParsesGuidDatetime2AndDecimal()
    {
        var plan = TestPlans.KitchenSink();
        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Native);

        using var data = TestFixtures.OpenEmbedded("KitchenSink.native_part1.bcp");
        var first = CoreAdapters.DecodeRows([data], plan, format).First();

        // Id
        first[0].Should().Be(1L);

        // TenantId - our decoder returns GUID as string "D" format
        first[1].Should().BeOfType<string>();
        ((string)first[1]!).Should().Be("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

        // CreatedAt - our decoder returns datetime2 as ISO 8601 string
        first[2].Should().BeOfType<string>();
        var dt = (string)first[2]!;
        dt.Should().Contain("2024-01-02");
        dt.Should().Contain("03:04:05");

        // Price - our decoder returns decimal as C# decimal
        first[3].Should().BeOfType<decimal>();
        ((decimal)first[3]!).Should().Be(123.4567m);

        // Payload - null
        first[4].Should().BeNull();

        // Title - null
        first[5].Should().BeNull();

        // IsActive
        first[6].Should().Be(1L);

        // Big
        first[7].Should().Be(9876543210L);
    }

    [Fact]
    public void Decode_KitchenSink_Row2_HandlesBlob()
    {
        var plan = TestPlans.KitchenSink();
        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Native);

        using var data = TestFixtures.OpenEmbedded("KitchenSink.native_part1.bcp");
        var rows = CoreAdapters.DecodeRows([data], plan, format).ToList();
        var row2 = rows[1];

        row2[0].Should().Be(2L);

        // Payload should be [DE AD BE EF]
        row2[4].Should().BeOfType<byte[]>();
        ((byte[])row2[4]!).Should().Equal(0xDE, 0xAD, 0xBE, 0xEF);

        // Title should be "Hello World"
        row2[5].Should().Be("Hello World");

        // IsActive = 0
        row2[6].Should().Be(0L);

        // Big = 0
        row2[7].Should().Be(0L);
    }

    [Fact]
    public void Decode_KitchenSink_Row3_EmptyStringVsNull()
    {
        var plan = TestPlans.KitchenSink();
        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Native);

        using var data = TestFixtures.OpenEmbedded("KitchenSink.native_part1.bcp");
        var rows = CoreAdapters.DecodeRows([data], plan, format).ToList();
        var row3 = rows[2];

        row3[0].Should().Be(3L);

        // Payload is null
        row3[4].Should().BeNull();

        // Title is empty string (not null)
        row3[5].Should().NotBeNull();
        row3[5].Should().Be("");

        // Big = -1
        row3[7].Should().Be(-1L);
    }

    [Fact]
    public void Decode_KitchenSink_Row4_MaxBigint()
    {
        var plan = TestPlans.KitchenSink();
        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Native);

        using var data = TestFixtures.OpenEmbedded("KitchenSink.native_part1.bcp");
        var rows = CoreAdapters.DecodeRows([data], plan, format).ToList();
        var row4 = rows[3];

        row4[7].Should().Be(9223372036854775807L); // long.MaxValue
    }

    [Fact]
    public void Decode_KitchenSink_DecimalPrecisionPreserved()
    {
        var plan = TestPlans.KitchenSink();
        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Native);

        using var data = TestFixtures.OpenEmbedded("KitchenSink.native_part1.bcp");
        var rows = CoreAdapters.DecodeRows([data], plan, format).ToList();

        // Row 2: price = 0.0001
        ((decimal)rows[1][3]!).Should().Be(0.0001m);

        // Row 3: price = 99999.9999
        ((decimal)rows[2][3]!).Should().Be(99999.9999m);

        // Row 4: price = 1.0000
        ((decimal)rows[3][3]!).Should().Be(1.0000m);
    }
}
