using System.Xml.Linq;
using BacpacToSqlite.Core.Planning;

namespace BacpacToSqlite.Core;

public static class ModelXmlParser
{
    public static ModelParseResult Parse(Stream modelXmlStream)
    {
        var doc = XDocument.Load(modelXmlStream);

        var modelElement = doc.Root?
            .Descendants()
            .FirstOrDefault(e => e.Name.LocalName == "Model");

        if (modelElement == null)
            throw new InvalidOperationException("Could not find Model element in model.xml");

        var tables = ParseTables(modelElement);
        var indices = ParseIndices(modelElement);
        var views = ParseViews(modelElement);

        return new ModelParseResult
        {
            Tables = tables,
            Indices = indices,
            Views = views
        };
    }

    private static Dictionary<(string Schema, string Table), TablePlan> ParseTables(XElement modelElement)
    {
        var result = new Dictionary<(string, string), TablePlan>();

        var tableElements = modelElement.Elements()
            .Where(e => e.Name.LocalName == "Element" &&
                        e.Attribute("Type")?.Value == "SqlTable");

        foreach (var tableEl in tableElements)
        {
            var tableName = tableEl.Attribute("Name")?.Value;
            if (tableName == null) continue;

            var (schema, table) = ParseSchemaAndName(tableName);

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

    private static IReadOnlyList<IndexPlan> ParseIndices(XElement modelElement)
    {
        var indices = new List<IndexPlan>();

        // SqlIndex - regular indices
        var indexElements = modelElement.Elements()
            .Where(e => e.Name.LocalName == "Element" &&
                        (e.Attribute("Type")?.Value == "SqlIndex" ||
                         e.Attribute("Type")?.Value == "SqlPrimaryKeyConstraint" ||
                         e.Attribute("Type")?.Value == "SqlUniqueConstraint"));

        foreach (var indexEl in indexElements)
        {
            var indexName = indexEl.Attribute("Name")?.Value;
            if (indexName == null) continue;

            var type = indexEl.Attribute("Type")!.Value;
            var isUnique = type is "SqlPrimaryKeyConstraint" or "SqlUniqueConstraint"
                || GetBoolProperty(indexEl, "IsUnique");

            // Get the defining table from Relationship[@Name="DefiningTable"]
            var definingTableRef = indexEl.Elements()
                .Where(e => e.Name.LocalName == "Relationship" &&
                            e.Attribute("Name")?.Value == "DefiningTable")
                .SelectMany(r => r.Elements().Where(x => x.Name.LocalName == "Entry"))
                .SelectMany(entry => entry.Elements().Where(x => x.Name.LocalName == "References"))
                .Select(refs => refs.Attribute("Name")?.Value)
                .FirstOrDefault();

            if (definingTableRef == null) continue;

            var (tableSchema, tableName) = ParseSchemaAndName(definingTableRef);

            // Get column names from Relationship[@Name="ColumnSpecifications"]
            var columnNames = ParseIndexColumns(indexEl);
            if (columnNames.Count == 0) continue;

            var (_, indexShortName) = ParseSchemaAndName(indexName);

            indices.Add(new IndexPlan
            {
                Name = indexShortName,
                Schema = tableSchema,
                TableName = tableName,
                ColumnNames = columnNames,
                IsUnique = isUnique
            });
        }

        return indices;
    }

    private static List<string> ParseIndexColumns(XElement indexElement)
    {
        var columns = new List<string>();

        var colSpecsRel = indexElement.Elements()
            .FirstOrDefault(e => e.Name.LocalName == "Relationship" &&
                                 e.Attribute("Name")?.Value == "ColumnSpecifications");

        if (colSpecsRel == null) return columns;

        var colSpecEntries = colSpecsRel.Elements()
            .Where(e => e.Name.LocalName == "Entry");

        foreach (var entry in colSpecEntries)
        {
            // Entry > Element[@Type="SqlIndexedColumnSpecification"] > Relationship[@Name="Column"] > Entry > References
            var colRef = entry.Elements()
                .Where(e => e.Name.LocalName == "Element")
                .SelectMany(el => el.Elements()
                    .Where(r => r.Name.LocalName == "Relationship" &&
                                r.Attribute("Name")?.Value == "Column"))
                .SelectMany(r => r.Elements().Where(x => x.Name.LocalName == "Entry"))
                .SelectMany(e => e.Elements().Where(x => x.Name.LocalName == "References"))
                .Select(refs => refs.Attribute("Name")?.Value)
                .FirstOrDefault();

            if (colRef != null)
            {
                // Column ref is like [schema].[table].[column] - take last part
                var colName = colRef.Split('.').Last().Trim('[', ']');
                columns.Add(colName);
            }
        }

        return columns;
    }

    private static IReadOnlyList<ViewPlan> ParseViews(XElement modelElement)
    {
        var views = new List<ViewPlan>();

        var viewElements = modelElement.Elements()
            .Where(e => e.Name.LocalName == "Element" &&
                        e.Attribute("Type")?.Value == "SqlView");

        foreach (var viewEl in viewElements)
        {
            var viewName = viewEl.Attribute("Name")?.Value;
            if (viewName == null) continue;

            var (schema, name) = ParseSchemaAndName(viewName);

            // Try QueryScript property (contains the SELECT statement)
            var queryScript = GetScriptProperty(viewEl, "QueryScript");

            // Try SelectStatement property as fallback
            if (queryScript == null)
                queryScript = GetScriptProperty(viewEl, "SelectStatement");

            // Try BodyScript relationship as another fallback
            if (queryScript == null)
                queryScript = GetBodyScript(viewEl);

            if (queryScript == null) continue;

            views.Add(new ViewPlan
            {
                Schema = schema,
                Name = name,
                SelectStatement = queryScript
            });
        }

        return views;
    }

    private static string? GetScriptProperty(XElement element, string propertyName)
    {
        var prop = element.Elements()
            .FirstOrDefault(e => e.Name.LocalName == "Property" &&
                                 e.Attribute("Name")?.Value == propertyName);

        if (prop == null) return null;

        // Script can be in Value attribute or child Value element
        var value = prop.Attribute("Value")?.Value;
        if (value != null) return value;

        var valueEl = prop.Elements()
            .FirstOrDefault(e => e.Name.LocalName == "Value");
        return valueEl?.Value;
    }

    private static string? GetBodyScript(XElement element)
    {
        return element.Elements()
            .Where(e => e.Name.LocalName == "Relationship" &&
                        e.Attribute("Name")?.Value == "BodyScript")
            .SelectMany(r => r.Elements().Where(x => x.Name.LocalName == "Entry"))
            .SelectMany(entry => entry.Elements().Where(x => x.Name.LocalName == "Element"))
            .Select(el => GetScriptProperty(el, "Script"))
            .FirstOrDefault(s => s != null);
    }

    private static bool GetBoolProperty(XElement element, string propertyName)
    {
        var prop = element.Elements()
            .FirstOrDefault(e => e.Name.LocalName == "Property" &&
                                 e.Attribute("Name")?.Value == propertyName);
        if (prop == null) return false;
        return bool.TryParse(prop.Attribute("Value")?.Value, out var val) && val;
    }

    private static (string Schema, string Name) ParseSchemaAndName(string fullName)
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
