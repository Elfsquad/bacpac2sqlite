using System.Globalization;
using System.Reflection;

namespace BacpacToSqlite.Tests.Fixtures;

public static class TestFixtures
{
    static TestFixtures()
    {
        CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
        CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;
    }

    public static Stream OpenEmbedded(string resourceSuffix)
    {
        var asm = Assembly.GetExecutingAssembly();
        var name = asm.GetManifestResourceNames()
            .FirstOrDefault(n => n.EndsWith(resourceSuffix, StringComparison.OrdinalIgnoreCase));

        if (name is null)
            throw new InvalidOperationException($"Embedded resource not found: *{resourceSuffix}. " +
                                                $"Available:\n- {string.Join("\n- ", asm.GetManifestResourceNames())}");

        return asm.GetManifestResourceStream(name)
            ?? throw new InvalidOperationException($"Failed to open embedded resource stream: {name}");
    }

    public static IEnumerable<Stream> OpenManyEmbedded(params string[] resourceSuffixes)
    {
        foreach (var suffix in resourceSuffixes)
            yield return OpenEmbedded(suffix);
    }

    public static Stream AsNonSeekable(Stream inner) => new NonSeekableStream(inner);

    private sealed class NonSeekableStream : Stream
    {
        private readonly Stream _inner;
        public NonSeekableStream(Stream inner) => _inner = inner;

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => _inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.ReadAsync(buffer, cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException("Seek is not allowed");
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
