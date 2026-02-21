using BacpacToSqlite.Core.BcpNative;
using BacpacToSqlite.Tests.Fixtures;
using BacpacToSqlite.Tests.Planning;
using FluentAssertions;

namespace BacpacToSqlite.Tests.BcpNative;

public class ProfileAutoDetectTests
{
    [Fact]
    public void Infer_AutoDetect_DefaultsToUnicodeNative()
    {
        var plan = TestPlans.KitchenSink();

        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Auto);

        format.Profile.Should().Be(BcpProfile.UnicodeNative);
    }

    [Fact]
    public void Infer_ForcedNative_ReturnsNativeProfile()
    {
        var plan = TestPlans.KitchenSink();

        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Native);

        format.Profile.Should().Be(BcpProfile.Native);
    }

    [Fact]
    public void Infer_AutoDetect_CanDecodeUnicodeNativeFixture()
    {
        var plan = TestPlans.KitchenSink();
        var format = CoreAdapters.InferFormat(Stream.Null, plan, BcpProfileOption.Auto);

        using var stream = TestFixtures.OpenEmbedded("KitchenSink.unicode_native_part1.bcp");
        var rows = CoreAdapters.DecodeRows([stream], plan, format).ToList();

        rows.Should().HaveCount(5);
        rows[0][0].Should().Be(1L);
    }
}
