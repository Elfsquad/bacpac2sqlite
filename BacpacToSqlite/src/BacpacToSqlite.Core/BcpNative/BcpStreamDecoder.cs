using BacpacToSqlite.Core.Planning;

namespace BacpacToSqlite.Core.BcpNative;

public static class BcpStreamDecoder
{
    public static IEnumerable<object?[]> DecodeRows(
        IEnumerable<Stream> bcpPartStreams,
        TablePlan plan,
        BcpFormatDescriptor format)
    {
        foreach (var stream in bcpPartStreams)
        {
            using var reader = new BcpRowReader(stream);

            while (true)
            {
                object?[] row;
                try
                {
                    row = ReadRow(reader, plan, format);
                }
                catch (EndOfStreamException)
                {
                    // Normal end of data within a part
                    break;
                }

                yield return row;
            }
        }
    }

    private static object?[] ReadRow(BcpRowReader reader, TablePlan plan, BcpFormatDescriptor format)
    {
        var row = new object?[plan.Columns.Count];

        for (int i = 0; i < plan.Columns.Count; i++)
        {
            var col = plan.Columns[i];
            var colFormat = format.Columns[i];
            row[i] = reader.ReadColumn(colFormat, col.Precision, col.Scale);
        }

        return row;
    }

}
