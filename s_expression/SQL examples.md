# SQL examples
This document contains examples for SQL and simplified SQL queries

## Value queries
*How many businesses and organizations went bankrupt in September 1993 after adjusting for court session days?*

#### With PIVOT:
```sql
SELECT *
FROM (
  SELECT
    Measure,
    Value,
    TypeOfBankruptcy,
    Periods
  FROM "data/en/odata3/83085ENG.parquet"
  UNPIVOT(Value FOR Measure IN ('BankruptciesSessionDayCorrected_1'))
)
PIVOT(MAX(Value) FOR TypeOfBankruptcy IN ('A047597') Periods IN ('1993MM09'));
```

Output:

| Measure                           | A047597_1993MM09 |
|-----------------------------------|------------------|
| BankruptciesSessionDayCorrected_1 | 293              |


#### Value SQL without PIVOT:
```sql
SELECT Periods, TypeOfBankruptcy, BankruptciesSessionDayCorrected_1
FROM "data/en/odata3/83085ENG.parquet"
WHERE TypeOfBankruptcy = 'A047597' AND Periods = '1993MM09';
```

Output:

| Periods  | TypeOfBankruptcy | BankruptciesSessionDayCorrected_1 |
|----------|------------------|-----------------------------------|
| 1993MM09 | A047597          | 293                               |



---


## Simple aggregators
### Case A: SUM on single measure
*How many mature laying hens and large fattening pigs were on farms in April 2022?*

#### With PIVOT
```sql
SELECT *
FROM (
  SELECT
    Measure,
    Value,
    Periods,
    FarmAnimals
  FROM "data/en/odata3/84952ENG.parquet"
 UNPIVOT(Value FOR Measure IN ('Livestock_1'))
 WHERE
   FarmAnimals IN ('A044005', 'A044022')
)
PIVOT(
    SUM(Value) FOR Periods IN ('2022MM04')
    GROUP BY Measure
);
```

Output:

| Measure     | 2022MM04 |
|-------------|----------|
| Livestock_1 | 4640     |


#### Without PIVOT
```sql
SELECT
  'Livestock_1' AS Measure,
  Periods,
  SUM(Livestock_1) AS 'SUM'
FROM "data/en/odata3/84952ENG.parquet"
WHERE FarmAnimals IN ('A044005', 'A044022')
AND Periods IN ('2022MM04')
GROUP BY Periods;
```

Output:

| Measure     | Periods  | SUM  |
|-------------|----------|------|
| Livestock_1 | 2022MM04 | 4640 |


### Case B: SUM on multiple measures
*Total municipal spending from both capital and current accounts for the years 1921 to 1925*

#### With PIVOT
```sql
SELECT
  *,
  'ExpenditureCapitalAccounts_8;ExpenditureCurrentAccounts_7' AS Measure
FROM (
  SELECT
    Value,
    Periods
  FROM "data/en/odata3/81206ENG.parquet"
  UNPIVOT(Value FOR Measure IN ('ExpenditureCapitalAccounts_8', 'ExpenditureCurrentAccounts_7'))
)
PIVOT(SUM(Value) FOR Periods IN ('1924JJ00', '1921JJ00', '1925JJ00', '1922JJ00', '1923JJ00'));
```

Output:

| 1924JJ00 | 1921JJ00 | 1925JJ00 | 1922JJ00 | 1923JJ00 | Measure                                                   |
|----------|----------|----------|----------|----------|-----------------------------------------------------------|
| 430      | 398      | 502      | 395      | 408      | ExpenditureCapitalAccounts_8;ExpenditureCurrentAccounts_7 |


#### Without PIVOT
```sql
SELECT 
  Periods,
  SUM(ExpenditureCapitalAccounts_8 + ExpenditureCurrentAccounts_7) AS 'SUM'
FROM "data/en/odata3/81206ENG.parquet"
WHERE Periods IN ('1924JJ00', '1921JJ00', '1925JJ00', '1922JJ00', '1923JJ00')
GROUP BY Periods;
```

Output:

| Periods  | SUM |
|----------|-----|
| 1923JJ00 | 408 |
| 1921JJ00 | 398 |
| 1925JJ00 | 502 |
| 1924JJ00 | 430 |
| 1922JJ00 | 395 |


### Case C: AVG on multiple measures and filtering on multiple dimensions
*average cost of hotel renovations started in 2019*

#### With **PIVOT**
```sql
SELECT
  *,
  'GITradeHotelsRestaurantsBars_7;BuildingCostsAlteration_2' AS Measure
FROM (
  SELECT
    Value,
    Periods,
    Region,
    BuildingPhases
  FROM "data/en/odata3/83707ENG.parquet"
  UNPIVOT(Value FOR Measure IN ('GITradeHotelsRestaurantsBars_7', 'BuildingCostsAlteration_2'))
)
PIVOT(AVG(Value) FOR 
  Periods IN ('2019JJ00')
  Region IN ('NL01')
  BuildingPhases IN ('A041324', 'A041322')
);
```

Output:

| 2019JJ00_NL01_A041324 | 2019JJ00_NL01_A041322 | Measure                                                  |
|-----------------------|-----------------------|----------------------------------------------------------|
| 2887                  | 2271                  | GITradeHotelsRestaurantsBars_7;BuildingCostsAlteration_2 |

#### Without PIVOT
```sql
SELECT
    Periods,
    Region,
    BuildingPhases,
    AVG(GITradeHotelsRestaurantsBars_7 + BuildingCostsAlteration_2) AS 'AVG'
FROM "data/en/odata3/83707ENG.parquet"
WHERE Periods IN ('2019JJ00')
AND Region IN ('NL01')
AND BuildingPhases IN ('A041324', 'A041322')
GROUP BY Periods, Region, BuildingPhases;
```

Output:

| Periods  | Region | BuildingPhases | AVG  |
|----------|--------|----------------|------|
| 2019JJ00 | NL01   | A041322        | 4542 |
| 2019JJ00 | NL01   | A041324        | 5774 |


---


### Case D: MIN over a dimension
*Which year between 2020 and 2022 had the lowest percentage of partnerships in accounting firms?*

#### With **PIVOT**
```sql
SELECT
  *
FROM (
  SELECT
    Measure,
    Value,
    LegalForms,
    Periods,
    SIC2008
  FROM "data/en/odata3/85201ENG.parquet"
  UNPIVOT(Value FOR Measure IN ('Total_1'))
  WHERE
    Periods IN ('2022JJ00', '2020JJ00', '2021JJ00')
)
PIVOT(MIN(Value) FOR LegalForms IN ('A050861') SIC2008 IN ('404200')
GROUP BY Measure);
```

Output:

| Measure | A050861_404200 |
|---------|----------------|
| Total_1 | 4452           |


#### Without PIVOT
```sql
WITH RankedRows AS (
    SELECT
        RANK() OVER (ORDER BY Total_1 ASC) as rnk,
        LegalForms,
        SIC2008,
        Total_1,
        Periods AS 'MIN[Periods]'
    FROM "data/en/odata3/85201ENG.parquet"
    WHERE LegalForms IN ('A050861')
    AND Periods IN ('2022JJ00', '2020JJ00', '2021JJ00')
    AND SIC2008 IN ('404200')
)
SELECT *
FROM RankedRows
WHERE rnk = 1;
```

Output:

| rnk | LegalForms | SIC2008 | Total_1 | MIN[Periods] |
|-----|------------|---------|---------|--------------|
| 1   | A050861    | 404200  | 4452    | 2022JJ00     |


### Case E: MIN over measures
*What is the lowest value among permits for building expansions and net change in building stocks for homes and non-residential properties, such as education and other facilities, in the Netherlands during the first quarter of 2025?*

#### With **PIVOT**
```sql
SELECT
  *,
  'BuildingPermitsAdditions_11;StockBalance_21' AS Measure
FROM (
  SELECT
    Value,
    Purpose,
    Period,
    Region
  FROM "data/en/odata3/86098ENG.parquet"
  UNPIVOT(Value FOR Measure IN ('BuildingPermitsAdditions_11', 'StockBalance_21'))
)
PIVOT(MIN(Value) FOR
  Purpose IN ('T001419', 'A045372', 'A045375')
  Period IN ('2025KW01')
  Region IN ('NL01')
);
```

Output:

| T001419_2025KW01_NL01 | A045372_2025KW01_NL01 | A045375_2025KW01_NL01 | Measure                                     |
|-----------------------|-----------------------|-----------------------|---------------------------------------------|
| 3804                  | -23                   | 34                    | BuildingPermitsAdditions_11;StockBalance_21 |


#### Without PIVOT
For doing the MIN + ARGMIN combination over measures requires a bit of a more elaborate solution, where we can't
eliminate/simplify the UNPIVOT found in the origional SQL unless we resolve to convoluted CASE-WHEN-THEN-ELSE shenanigans.
```sql
WITH RankedRows AS (
    SELECT
        RANK() OVER (ORDER BY Value ASC) as rnk,
        Measure,
        Purpose,
        Period,
        Region,
        Value AS 'MIN'
    FROM "data/en/odata3/86098ENG.parquet"
    UNPIVOT(Value FOR Measure IN ('BuildingPermitsAdditions_11', 'StockBalance_21'))
    WHERE Purpose IN ('T001419', 'A045372', 'A045375')
    AND Period IN ('2025KW01')
    AND Region IN ('NL01')
)
SELECT *
FROM RankedRows
WHERE rnk = 1;
```

Output:

| rnk | Measure         | Purpose | Period   | Region | MIN |
|-----|-----------------|---------|----------|--------|-----|
| 1   | StockBalance_21 | A045372 | 2025KW01 | NL01   | -23 |


---


## PROP
*What proportion of the population in 1916 was there compared to both 1915 and 1916?*

The PROP-type queries already don't contain any PIVOT clauses in their current form.
#### Current version
```sql
SELECT
  CONCAT_WS(', ', Measure) AS Dimension_Measure,
  SUM(Value) AS "SUM['Periods']",
  SUM(CASE WHEN Periods IN ('1916JJ00') THEN Value ELSE 0 END) AS "1916JJ00",
  ROUND(
    SUM(CASE WHEN Periods IN ('1916JJ00') THEN Value ELSE 0 END) * 100.0 / SUM(Value),
    2
  ) AS "%"
FROM (
  SELECT
    Measure,
    Value,
    Periods
  FROM "data/en/odata3/37852eng.parquet"
  UNPIVOT(Value FOR Measure IN (PopulationOnJanuary1_1))
)
WHERE
  Periods IN ('1915JJ00', '1916JJ00')
GROUP BY
  Dimension_Measure;
```

---


## JOIN
*How many companies and individuals went bankrupt and how many homes were sold in the last quarter of 2007?*

#### With **PIVOT**
```sql
SELECT
  TableA.Periods,
  A041718_BankruptciesSessionDayCorrected_1,
  A047597_BankruptciesSessionDayCorrected_1,
  SoldHomes_4
FROM (
  SELECT
    *
  FROM (
    SELECT
      Measure,
      Value,
      Periods,
      TypeOfBankruptcy
    FROM "data/en/odata3/83085ENG.parquet"
    UNPIVOT(Value FOR Measure IN ('BankruptciesSessionDayCorrected_1'))
    WHERE
      Periods IN ('2007KW04')
  )
  PIVOT(MAX(Value) FOR
    TypeOfBankruptcy IN ('A041718', 'A047597')
    Measure IN ('BankruptciesSessionDayCorrected_1')
  )
) AS TableA
FULL JOIN (
  SELECT
    *
  FROM (
    SELECT
      Measure,
      Value,
      Periods
    FROM "data/en/odata3/85773ENG.parquet"
    UNPIVOT(Value FOR Measure IN ('SoldHomes_4'))
    WHERE
      Periods IN ('2007KW04')
  )
  PIVOT(MAX(Value) FOR Measure IN ('SoldHomes_4'))
) AS TableB
  ON TableA.Periods = TableB.Periods;
```

Output:

| Periods  | A041718_BankruptciesSessionDayCorrected_1 | A047597_BankruptciesSessionDayCorrected_1 | SoldHomes_4 |
|----------|-------------------------------------------|-------------------------------------------|-------------|
| 2007KW04 | 722                                       | 871                                       | 54562       |


#### Without PIVOT (simplified for the inner queries)
```sql
SELECT
    COALESCE(TableA.Periods, TableB.Periods) AS 'Periods',
    BankruptciesSessionDayCorrected_1,
    SoldHomes_4
FROM (
    SELECT *
    FROM "data/en/odata3/83085ENG.parquet"
    WHERE Periods IN ('2007KW04')
    AND TypeOfBankruptcy IN ('A041718', 'A047597')
) AS TableA
FULL JOIN (
    SELECT *
    FROM "data/en/odata3/85773ENG.parquet"
    WHERE Periods IN ('2007KW04')
) AS TableB
ON TableA.Periods = TableB.Periods;
```

Output:

| Periods  | BankruptciesSessionDayCorrected_1 | SoldHomes_4 |
|----------|-----------------------------------|-------------|
| 2007KW04 | 722                               | 54562       |
| 2007KW04 | 871                               | 54562       |

A pivoted version can be achieved by changing `BankruptciesSessionDayCorrected_1` in the SELECT clause by
```sql
MAX(CASE WHEN TypeOfBankruptcy = 'A041718' THEN BankruptciesSessionDayCorrected_1 END) AS A041718_BankruptciesSessionDayCorrected_1,
MAX(CASE WHEN TypeOfBankruptcy = 'A047597' THEN BankruptciesSessionDayCorrected_1 END) AS A047597_BankruptciesSessionDayCorrected_1,
```
and removing it from the GROUP BY clause. This will result in the following ouput:

| Periods  | A041718_BankruptciesSessionDayCorrected_1 | A047597_BankruptciesSessionDayCorrected_1 | SoldHomes_4 |
|----------|-------------------------------------------|-------------------------------------------|-------------|
| 2007KW04 | 722                                       | 871                                       | 54562       |

---


## AGGJOIN
*What is the average typical electricity usage per square meter for offices, motor companies and sports facilities built after 1994 and office locations with smaller and medium-sized floor space?*

#### With **PIVOT**
```sql
SELECT
    Measure,
    AVG(Value) AS AVG
FROM (
    SELECT *
    FROM (
        SELECT *
        FROM (
            SELECT
                Measure,
                Value,
                ConstructionPeriod,
                FloorareaSize,
                BuildingTypesServicesSector
            FROM "data/en/odata3/83376ENG.parquet"
            UNPIVOT(Value FOR Measure IN ('AverageConsumptionOfElectricity_2'))
            WHERE Measure IN ('AverageConsumptionOfElectricity_2')
        )
        PIVOT(MAX(Value) FOR 
            ConstructionPeriod IN ('ZW25814')
            FloorareaSize IN ('A025415')
            BuildingTypesServicesSector IN ('A047460', 'A047461', 'A047477')
        )
    ) AS TableA
    FULL JOIN (
        SELECT *
        FROM (
            SELECT
                Measure,
                Value,
                FloorArea,
                EnergyIntensitiesServicesFloorArea
            FROM "data/en/odata3/83374ENG.parquet"
            UNPIVOT(Value FOR Measure IN ('AverageConsumptionOfElectricity_2'))
            WHERE Measure IN ('AverageConsumptionOfElectricity_2')
        )
        PIVOT(MAX(Value) FOR FloorArea IN ('A025413') EnergyIntensitiesServicesFloorArea IN ('A047466'))
    ) AS TableB
    ON TableA.Measure = TableB.Measure
)
UNPIVOT(Value FOR name IN (COLUMNS(c -> NOT CONTAINS(c, 'Measure'))))
GROUP BY Measure
```

Output:

| Measure                           | AVG    |
|-----------------------------------|--------|
| AverageConsumptionOfElectricity_2 | 58.475 |


#### Without Pivot
```sql
SELECT
    'AverageConsumptionOfElectricity_2' AS Measure,
    AVG(AverageConsumptionOfElectricity_2) AS AVG
FROM (
    SELECT
        COALESCE(TableA.AverageConsumptionOfElectricity_2, TableB.AverageConsumptionOfElectricity_2) AS "AverageConsumptionOfElectricity_2",
        TableA.FloorareaSize,
        TableA.BuildingTypesServicesSector,
        TableA.ConstructionPeriod,
        TableB.FloorArea,
        TableB.EnergyIntensitiesServicesFloorArea
    FROM (
        SELECT *
        FROM "data/en/odata3/83376ENG.parquet"
        WHERE FloorareaSize = 'A025415'
        AND BuildingTypesServicesSector IN ('A047460', 'A047461', 'A047477')
        AND ConstructionPeriod = 'ZW25814'
    ) AS TableA
    FULL JOIN (
      SELECT *
      FROM "data/en/odata3/83374ENG.parquet"
      WHERE FloorArea = 'A025413' AND EnergyIntensitiesServicesFloorArea = 'A047466'
    ) AS TableB
    ON TableA.AverageConsumptionOfElectricity_2 = TableB.AverageConsumptionOfElectricity_2
)
GROUP BY Measure;
```

Output:

| Measure                           | AVG    |
|-----------------------------------|--------|
| AverageConsumptionOfElectricity_2 | 58.475 |
