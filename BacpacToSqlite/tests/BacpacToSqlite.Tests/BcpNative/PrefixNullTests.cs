using BacpacToSqlite.Tests.Fixtures;
using BacpacToSqlite.Tests.Planning;
using FluentAssertions;

namespace BacpacToSqlite.Tests.BcpNative;

public class PrefixNullTests
{
    [Fact]
    public void Decode_PrefixNulls_NativeProfile_HandlesNullVsEmpty()
    {
        var plan = TestPlans.PrefixNulls();
        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Native);

        using var stream = TestFixtures.OpenEmbedded("PrefixNulls.native_part1.bcp");
        var rows = CoreAdapters.DecodeRows([stream], plan, format).ToList();

        rows.Should().HaveCount(2);

        // Row 1: K=1, VarcharMaybe=NULL, VarbinaryMaybe=NULL
        rows[0][0].Should().Be(1L);
        rows[0][1].Should().BeNull();
        rows[0][2].Should().BeNull();

        // Row 2: K=2, VarcharMaybe="" (empty string, not null), VarbinaryMaybe=[01,02,03]
        rows[1][0].Should().Be(2L);
        rows[1][1].Should().Be(""); // empty string must not become null
        ((byte[]?)rows[1][2]).Should().Equal(new byte[] { 0x01, 0x02, 0x03 });
    }

    [Fact]
    public void Decode_PrefixNulls_WorksWithNonSeekableStream()
    {
        var plan = TestPlans.PrefixNulls();
        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Native);

        using var raw = TestFixtures.OpenEmbedded("PrefixNulls.native_part1.bcp");
        using var nonSeek = TestFixtures.AsNonSeekable(raw);
        var rows = CoreAdapters.DecodeRows([nonSeek], plan, format).ToList();

        rows.Should().HaveCount(2);
        rows[0][0].Should().Be(1L);
        rows[0][1].Should().BeNull();
    }
}
