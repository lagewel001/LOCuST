# GECKO Complex S-expressions syntaxis
This document contains the syntax description of the custom S-expressions Lisp-based query language for querying OData tables.

### Notes
- S-expressions are constrained to have only one aggregation function. This means that you can't JOIN two intermediary outputs of two different aggregations. For example,
  ```
  (JOIN (SUM ... ) (AVG ... ) (Perioden (2022 2023)))
  ```
  would be invalid. In short, the aggregation function always denotes the last operation done over one or multiple retrieved value outputs (i.e. the `VALUE` function). Multi-joins can be possible but won't be contained in our training dataset.
- Functions that are not yet worked out here:
  - Difference (`DIFF`)/Subtraction (`SUB`)/Comparison (`COMP`); these operators are tricky because it is required to know the base value and the operator value. E.g. do you subtract A from B or B from A?
- The codes used in the example expressions below are mostly dummy codes for more easy readability.
- The expression outputs given for the examples are just indicative. The actual output can differ slightly.

## Syntax table

| Sub-expression | Syntax                                                                                       |
|:----:|:---------------------------------------------------------------------------------------------|
| tab | `<code>`                                                                                     |
| msrs | `(MSR (<code₁> <code>))` <br> `(MSR (<code₁> <op> <int/float>))`*                            |
| dim | `(DIM <codegroup> (<code₁> <code>))` <br> `(DIM <codegroup> ())`                             |
| dims | `(dim₁ ... dimn)`                                                                            |
| val | `(VALUE tab msrs dims)`                                                                      |
| sum | `(SUM () val)` <br> `(SUM <codegroup> val)`                                                  |
| avg | `(AVG () val)` <br> `(AVG <codegroup> val)`                                                  |
| max/min | `(MAX () val)` <br> `(MIN () val)` <br> `(MAX <codegroup> val)` <br> `(MIN <codegroup> val)` |
| prop | `(PROP dim val)`                                                                             |
| join | `(JOIN <codegroup> val₁ val₂)`                                                               |

Aggregation functions = { `VALUE`, `SUM`, `AVG`, `MIN`, `MAX`, `PROP`, `JOIN` }

*Comparison operators = { `<`, `>`, `!=`, `<=`, `>=`, `=` }

## VALUE

### Notes
- `VALUE` denotes the operation of retrieving, filtering and selecting values from OData. The raw values of what is in OData are returned, depending on the selected measure and dimension filters. The output of one or multiple `VALUE` functions can be used in an aggregation function.
- When filtering using a comparison operator, only one measure can be selected for a given `VALUE` expression. 
- 'Between' queries are not possible at this point (e.g. `(MSR (25 < <code> <= 50))`)
- When filtering on a dimension without providing codes `(DIM <codegroup> ())`, all codes are selected for that dimension group. This can be useful when getting all Periods for a given measure.
 
### Examples
```
(VALUE 85440NED
  (MSR (D001607 D001636))
  (DIM GN (GN01012100 GN01012910))
  (DIM Landen (Totaal landen))
  (DIM Perioden (2023))
)
```

### Retrieval output:
*VALUE*

| GN | Perioden | Grensoverschrijving goederen; invoer Totale invoerwaarde Totaal landen | Grensoverschrijving goederen; uitvoer Totale uitvoerwaarde Totaal landen |
| :--- | :--- | :--- | :--- |
| 01012990 Paarden, levend (m.u.v. fokp... | 2023** | 20 | 423 |
| 01022910 Rundvee, levend, met een gew... | 2023** | 156 | 18 |

### Expression output:

| MSR | Perioden | Grensoverschrijving goederen; invoer Totale invoerwaarde Totaal Landen | Grensoverschrijving goederen; uitvoer Totale invoerwaarde Totaal landen |
| :--- | :--- | :--- | :--- |
| Paarden | 2023 | 20 | 423 |
| Rundvee | 2023 | 156 | 18 |


## SUM

### Notes
If selecting measure codes as a summation selector, the sum should only be possible when all selected measures share the same unit of measurement

### Examples
*SUM Example 1*
```
(SUM
  ()
  (VALUE 85440NED
    (MSR (D001607 D001636))
    (DIM GN (GN01012100 GN01012910))
    (DIM Landen (Totaal landen))
    (DIM Perioden (2023))
  )
)
```

### Retrieval output:
*VALUE*

| GN | Perioden | Grensoverschrijving goederen; invoer Totale invoerwaarde Totaal landen | Grensoverschrijving goederen; uitvoer Totale uitvoerwaarde Totaal landen |
| :--- | :--- | :--- | :--- |
| 01012990 Paarden, levend (m.u.v. fokp... | 2023** | 20 | 423 |
| 01022910 Rundvee, levend, met een gew... | 2023** | 156 | 18 |

### Expression output:

| MSR | Perioden | Grensoverschrijving goederen; invoer Totale invoerwaarde Totaal Landen | Grensoverschrijving goederen; uitvoer Totale invoerwaarde Totaal landen |
|:----| :--- | :--- | :--- |
| SUM | 2023 | 176 | 441 |

---
*SUM Example 2*
```
(SUM
  (Perioden)
  (VALUE 85440NED
    (MSR (D001607 D001636))
    (DIM GN (GN01012100 GN01012910))
    (DIM Landen (Totaal landen))
    (DIM Perioden (2021 2022 2023))
  )
)
```

### Retrieval output:
*VALUE*

| GN | Perioden | Grensoverschrijving goederen; invoer Totale invoerwaarde Totaal landen | Grensoverschrijving goederen; uitvoer Totale uitvoerwaarde Totaal landen |
| :--- | :--- | :--- | :--- |
| 01012990 Paarden, levend (m.u.v. fokp... | 2021 | | |
| | 2022 | 11 | 411 |
| | 2023** | 20 | 423 |
| 01022910 Rundvee, levend, met een gew... | 2021 | | |
| | 2022 | 136 | 20 |
| | 2023** | 156 | 18 |


### Expression output:

| Perioden    | Grensoverschrijving goederen; invoer Totale invoerwaarde Totaal Landen | Grensoverschrijving goederen; uitvoer Totale invoerwaarde Totaal landen |
|:------------| :--- | :--- |
| Paarden SUM | 31 | 834 |
| Rundvee SUM | 292 | 38 |


## AVG

### Notes
Same constraints as applicable for the `SUM` function

### Examples
```
(AVG
  ()
  (VALUE 85440NED
    (MSR (D001607 D001636))
    (DIM GN (GN01012100 GN01012910))
    (DIM Landen (Totaal landen))
    (DIM Perioden (2023))
  )
)
```

### Retrieval output:
*VALUE*

| GN | Perioden | Grensoverschrijving goederen; invoer Totale invoerwaarde Totaal landen | Grensoverschrijving goederen; uitvoer Totale uitvoerwaarde Totaal landen |
| :--- | :--- | :--- | :--- |
| 01012990 Paarden, levend (m.u.v. fokp... | 2023** | 20 | 423 |
| 01022910 Rundvee, levend, met een gew... | 2023** | 156 | 18 |

### Expression output:

| MSR | Perioden | Grensoverschrijving goederen; invoer Totale invoerwaarde Totaal Landen | Grensoverschrijving goederen; uitvoer Totale invoerwaarde Totaal landen |
| :--- | :--- | :--- | :--- |
| AVG | 2023 | 88 | 220,5 |


## MAX/MIN

### Notes
`MAX` and `MIN` work like a combined MAX/MIN and ARGMAX/ARGMIN function. This means that always the context is returned from the initial tables (see example below).

### Examples
```
(MAX
  (Perioden)
  (VALUE 80780ned
    (MSR (A029003_2))
    (DIM RegioS (NL01))
    (DIM Perioden (2021 2022 2023))
  )
)
```

### Retrieval output:
*VALUE*

| Regio's | Perioden | Graasdieren Aantal dieren Schapen |
| :--- | :--- | :--- |
| Nederland | 2021 | 860 151 |
| | 2022 | 854 220 |
| | 2023 | 838 586 |

### Expression output:

| MSR | Nederland 2023 |
| :--- | :--- |
| Schapen, totaal | 838.586 |


## PROP

### Notes
`PROP` returns the proportion of a given dimension code compared to the total (sum) of the entire dimension. E.g. it would return the percentage of a specific given year compared to the sum of all years in the filtered VALUE expression. In practice, this means that `PROP` executes a `SUM` first, before calculating the proportion of the given selector.

### Examples
```
(PROP
  (DIM Perioden (2021))
  (VALUE 85302NED
    (MSR (D004645))
    (DIM Perioden (2021 2022))
    (DIM BestemmingEnSeizoen (L008691 L999996))
    (DIM Vakantiekenmerken (T001460))
    (DIM Marges (MW00000))
  )
)
```

### Retrieval output:
*VALUE*

| Perioden | Bestemming en seizoen | Vakantiekenmerken | Marges | Totaal vakanties |
| :--- | :--- | :--- | :--- | :--- |
| 2021 | Totaal vakanties | Totaal vakanties | Waarde | 31 685 |
| 2022 | Totaal vakanties | Totaal vakanties | Waarde | 35 933 |

### Expression output:

| | X 1 000 Totaal vakanties | Totaal 2021 | % |
| :--- | :--- | :--- | :--- |
| Vakantiebestemming: Nederland Totaal vakanties Totaal 2021 | 37.938 | 21.440 | 56,51 |
| Vakantiebestemming: buitenland Totaal vakanties Totaal 2021 | 29.681 | 21.440 | 72,23 |


## JOIN

### Notes
When joining two `VALUE` outputs, all the selected joining filters must be present in both individual result sets from the `VALUE` functions (similar to the JOIN-ON operator in SQL).

### Examples
```
(JOIN
  (Perioden)
  (VALUE 70675ned)
    (MSR (Huurverhoging))
    (DIM Perioden (2022 2023 2024))
  )
  (VALUE 70936ned)
    (MSR (Jaarmutatie CPI))
    (DIM Perioden (2022 2023 2024))
  )
)
```

### Retrieval output:
*VALUE 1*

| Perioden | Huurverhoging % |
| :--- | :--- |
| 2022 | 3,0 |
| 2023 | 2,0 |
| 2024 | 5,4 |

*VALUE 2*

| Perioden | Jaarmutatie CPI % |
| :--- | :--- |
| 2022 | 10,0 |
| 2023 | 3,8 |
| 2024* | 3,3 |

### Expression output:

| MSR | Perioden 2022 | 2023 | 2024 |
| :--- | :--- | :--- | :--- |
| Huurverhoging | 3,0 | 2,0 | 5,4 |
| Jaarmutatie CPI | 10,0 | 3,8 | 3,3 |
