using BacpacToSqlite.Tests.Fixtures;
using BacpacToSqlite.Tests.Planning;
using FluentAssertions;

namespace BacpacToSqlite.Tests.BcpNative;

public class MultiPartConcatenationTests
{
    [Fact]
    public void Decode_MultiPart_Bcp_ProducesAllRowsInOrder()
    {
        var plan = TestPlans.KitchenSink();
        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Native);

        using var p1 = TestFixtures.OpenEmbedded("KitchenSink.native_part1.bcp");
        using var p2 = TestFixtures.OpenEmbedded("KitchenSink.native_part2.bcp");

        var rows = CoreAdapters.DecodeRows([p1, p2], plan, format).ToList();

        rows.Should().HaveCount(10);

        // Ids should be 1..10 in order
        var ids = rows.Select(r => (long)r[0]!).ToList();
        ids.Should().BeInAscendingOrder();
        ids.Should().Equal(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    }

    [Fact]
    public void Decode_SinglePart_ProducesCorrectCount()
    {
        var plan = TestPlans.KitchenSink();
        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Native);

        using var p1 = TestFixtures.OpenEmbedded("KitchenSink.native_part1.bcp");
        var rows = CoreAdapters.DecodeRows([p1], plan, format).ToList();

        rows.Should().HaveCount(5);
    }
}
