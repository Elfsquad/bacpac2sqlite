using System.IO.Compression;

namespace BacpacToSqlite.Core;

public sealed class EntryInfo
{
    public required string FullName { get; init; }
    public required long CompressedLength { get; init; }
    public required long Length { get; init; }
}

public sealed class BacpacArchiveReader : IDisposable
{
    private readonly ZipArchive _archive;

    public BacpacArchiveReader(string bacpacPath)
    {
        var stream = File.OpenRead(bacpacPath);
        _archive = new ZipArchive(stream, ZipArchiveMode.Read);
    }

    public IReadOnlyList<EntryInfo> ListEntries()
    {
        return _archive.Entries.Select(e => new EntryInfo
        {
            FullName = e.FullName,
            CompressedLength = e.CompressedLength,
            Length = e.Length
        }).ToList();
    }

    public Stream OpenEntry(string entryName)
    {
        var entry = _archive.GetEntry(entryName)
            ?? throw new InvalidOperationException($"Entry '{entryName}' not found in archive.");
        return entry.Open();
    }

    public EntryInfo? FindEntryByName(string name)
    {
        var entry = _archive.Entries.FirstOrDefault(e =>
            e.FullName.Equals(name, StringComparison.OrdinalIgnoreCase));
        if (entry == null) return null;
        return new EntryInfo
        {
            FullName = entry.FullName,
            CompressedLength = entry.CompressedLength,
            Length = entry.Length
        };
    }

    public IReadOnlyList<EntryInfo> FindEntriesBySuffix(string suffix)
    {
        return _archive.Entries
            .Where(e => e.FullName.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
            .Select(e => new EntryInfo
            {
                FullName = e.FullName,
                CompressedLength = e.CompressedLength,
                Length = e.Length
            })
            .ToList();
    }

    public byte[] ReadEntryBytes(string entryName, int maxBytes)
    {
        using var stream = OpenEntry(entryName);
        var buffer = new byte[maxBytes];
        var bytesRead = stream.ReadAtLeast(buffer, maxBytes, throwOnEndOfStream: false);
        return buffer[..bytesRead];
    }

    public void Dispose() => _archive.Dispose();
}
