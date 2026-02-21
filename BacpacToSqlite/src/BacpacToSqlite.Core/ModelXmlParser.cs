using System.Xml.Linq;
using BacpacToSqlite.Core.Planning;

namespace BacpacToSqlite.Core;

public static class ModelXmlParser
{
    public static Dictionary<(string Schema, string Table), TablePlan> Parse(Stream modelXmlStream)
    {
        var doc = XDocument.Load(modelXmlStream);
        var result = new Dictionary<(string, string), TablePlan>();

        var modelElement = doc.Root?
            .Descendants()
            .FirstOrDefault(e => e.Name.LocalName == "Model");

        if (modelElement == null)
            throw new InvalidOperationException("Could not find Model element in model.xml");

        var tableElements = modelElement.Elements()
            .Where(e => e.Name.LocalName == "Element" &&
                        e.Attribute("Type")?.Value == "SqlTable");

        foreach (var tableEl in tableElements)
        {
            var tableName = tableEl.Attribute("Name")?.Value;
            if (tableName == null) continue;

            var (schema, table) = ParseSchemaAndTable(tableName);

            var columns = ParseColumns(tableEl);
            if (columns.Count == 0) continue;

            result[(schema, table)] = new TablePlan
            {
                Schema = schema,
                Name = table,
                Columns = columns
            };
        }

        return result;
    }

    private static (string Schema, string Table) ParseSchemaAndTable(string fullName)
    {
        var parts = fullName.Split('.')
            .Select(p => p.Trim('[', ']'))
            .ToArray();

        return parts.Length >= 2
            ? (parts[^2], parts[^1])
            : ("dbo", parts[^1]);
    }

    private static List<ColumnPlan> ParseColumns(XElement tableElement)
    {
        var columns = new List<ColumnPlan>();
        int ordinal = 0;

        // Real structure: Table > Relationship[@Name="Columns"] > Entry > Element[@Type="SqlSimpleColumn"]
        var columnsRel = tableElement.Elements()
            .FirstOrDefault(e => e.Name.LocalName == "Relationship" &&
                                 e.Attribute("Name")?.Value == "Columns");

        if (columnsRel == null) return columns;

        // Only include SqlSimpleColumn; computed columns are not stored in BCP data
        var columnElements = columnsRel.Elements()
            .Where(e => e.Name.LocalName == "Entry")
            .SelectMany(entry => entry.Elements())
            .Where(e => e.Name.LocalName == "Element" &&
                        e.Attribute("Type")?.Value == "SqlSimpleColumn");

        foreach (var colEl in columnElements)
        {
            var colFullName = colEl.Attribute("Name")?.Value;
            if (colFullName == null) continue;

            var colName = colFullName.Split('.').Last().Trim('[', ']');

            var typeSpecEl = GetTypeSpecifierElement(colEl);
            var sqlType = GetSqlTypeFromTypeSpecifier(typeSpecEl, colEl);
            var isNullable = GetColumnNullability(colEl);
            var maxLength = GetMaxLength(typeSpecEl);
            var precision = GetTypeSpecPropertyInt(typeSpecEl, "Precision", 0);
            var scale = GetTypeSpecPropertyInt(typeSpecEl, "Scale", 0);

            columns.Add(new ColumnPlan
            {
                Name = colName,
                Ordinal = ordinal++,
                SqlType = sqlType,
                IsNullable = isNullable,
                MaxLength = maxLength,
                Precision = precision,
                Scale = scale
            });
        }

        return columns;
    }

    private static XElement? GetTypeSpecifierElement(XElement colElement)
    {
        // Column > Relationship[@Name="TypeSpecifier"] > Entry > Element[@Type="SqlTypeSpecifier"]
        return colElement.Elements()
            .Where(e => e.Name.LocalName == "Relationship" &&
                        e.Attribute("Name")?.Value == "TypeSpecifier")
            .SelectMany(r => r.Elements().Where(x => x.Name.LocalName == "Entry"))
            .SelectMany(entry => entry.Elements().Where(x => x.Name.LocalName == "Element"))
            .FirstOrDefault();
    }

    private static string GetSqlTypeFromTypeSpecifier(XElement? typeSpecEl, XElement colElement)
    {
        if (typeSpecEl != null)
        {
            // TypeSpecifier > Relationship[@Name="Type"] > Entry > References/@Name
            var refName = typeSpecEl.Elements()
                .Where(e => e.Name.LocalName == "Relationship" && e.Attribute("Name")?.Value == "Type")
                .SelectMany(r => r.Elements().Where(x => x.Name.LocalName == "Entry"))
                .SelectMany(entry => entry.Elements().Where(x => x.Name.LocalName == "References"))
                .Select(refs => refs.Attribute("Name")?.Value)
                .FirstOrDefault();

            if (refName != null)
                return refName.Split('.').Last().Trim('[', ']').ToLowerInvariant();
        }

        // Fallback: direct Relationship[@Name="Type"] on the column (for some schemas)
        var directRef = colElement.Elements()
            .Where(e => e.Name.LocalName == "Relationship" && e.Attribute("Name")?.Value == "Type")
            .SelectMany(r => r.Elements().Where(x => x.Name.LocalName == "Entry"))
            .SelectMany(entry => entry.Elements().Where(x => x.Name.LocalName == "References"))
            .Select(refs => refs.Attribute("Name")?.Value)
            .FirstOrDefault();

        if (directRef != null)
            return directRef.Split('.').Last().Trim('[', ']').ToLowerInvariant();

        return "nvarchar";
    }

    private static bool GetColumnNullability(XElement colElement)
    {
        var prop = colElement.Elements()
            .FirstOrDefault(e => e.Name.LocalName == "Property" &&
                                 e.Attribute("Name")?.Value == "IsNullable");
        if (prop == null) return true; // default nullable in SQL Server
        return bool.TryParse(prop.Attribute("Value")?.Value, out var val) && val;
    }

    private static int GetMaxLength(XElement? typeSpecEl)
    {
        if (typeSpecEl == null) return -1;

        // Check for IsMax property
        var isMax = typeSpecEl.Elements()
            .FirstOrDefault(e => e.Name.LocalName == "Property" &&
                                 e.Attribute("Name")?.Value == "IsMax");
        if (isMax != null && isMax.Attribute("Value")?.Value == "True")
            return -1;

        // Check for Length property
        var length = typeSpecEl.Elements()
            .FirstOrDefault(e => e.Name.LocalName == "Property" &&
                                 e.Attribute("Name")?.Value == "Length");
        if (length != null && int.TryParse(length.Attribute("Value")?.Value, out var val))
            return val;

        return -1;
    }

    private static int GetTypeSpecPropertyInt(XElement? typeSpecEl, string propertyName, int defaultValue)
    {
        if (typeSpecEl == null) return defaultValue;

        var prop = typeSpecEl.Elements()
            .FirstOrDefault(e => e.Name.LocalName == "Property" &&
                                 e.Attribute("Name")?.Value == propertyName);
        if (prop == null) return defaultValue;
        return int.TryParse(prop.Attribute("Value")?.Value, out var val) ? val : defaultValue;
    }
}
