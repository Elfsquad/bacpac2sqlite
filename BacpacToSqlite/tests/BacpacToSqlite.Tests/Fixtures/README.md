# BCP Test Fixtures

This directory holds BCP native binary files used for testing the decoder.

## Generating Fixtures

You need a SQL Server instance to generate fixtures. Connect and run:

```sql
-- Create a test table with common types
CREATE TABLE dbo.TestAllTypes (
    Id int NOT NULL,
    TinyVal tinyint NULL,
    SmallVal smallint NULL,
    BigVal bigint NULL,
    BitVal bit NULL,
    RealVal real NULL,
    FloatVal float NULL,
    GuidVal uniqueidentifier NULL,
    VarcharVal varchar(100) NULL,
    NVarcharVal nvarchar(100) NULL,
    VarbinaryVal varbinary(100) NULL,
    DateVal date NULL,
    DateTime2Val datetime2(7) NULL,
    DecimalVal decimal(18,4) NULL,
    NullInt int NULL
);

INSERT INTO dbo.TestAllTypes VALUES
(1, 255, -32000, 9876543210, 1, 3.14, 2.718281828, 'A1B2C3D4-E5F6-7890-ABCD-EF1234567890',
 'hello', N'world', 0xDEADBEEF, '2024-06-15', '2024-06-15T13:45:30.1234567', 12345.6789, NULL);
```

Then export:

```bash
# Native format (-n)
bcp dbo.TestAllTypes out native.bcp -n -S localhost -d testdb -U sa -P yourpassword

# Unicode-native format (-N)
bcp dbo.TestAllTypes out unicode_native.bcp -N -S localhost -d testdb -U sa -P yourpassword

# Generate format file for reference
bcp dbo.TestAllTypes format nul -n -f native.fmt -S localhost -d testdb -U sa -P yourpassword
```

Place the generated `.bcp` and `.fmt` files in this directory.
