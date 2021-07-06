# USUKSanc

**US and UK Sanctions**

A union of sanctioned individuals and entities from the US Office of Foreign Assets Control and the UK Treasury.

## How to run

`spark-submit --packages com.databricks:spark-xml_2.12:0.12.0 --master local[*] app.py`

## Schemata

### US

The Office of Foreign Assets Control (OFAC)'s Specially Designated Nationals (SDN) list uses the following schema:

```
root
 |-- addressList: struct (nullable = true)
 |    |-- address: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- address1: string (nullable = true)
 |    |    |    |-- address2: string (nullable = true)
 |    |    |    |-- address3: string (nullable = true)
 |    |    |    |-- city: string (nullable = true)
 |    |    |    |-- country: string (nullable = true)
 |    |    |    |-- postalCode: string (nullable = true)
 |    |    |    |-- stateOrProvince: string (nullable = true)
 |    |    |    |-- uid: long (nullable = true)
 |-- akaList: struct (nullable = true)
 |    |-- aka: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- category: string (nullable = true)
 |    |    |    |-- firstName: string (nullable = true)
 |    |    |    |-- lastName: string (nullable = true)
 |    |    |    |-- type: string (nullable = true)
 |    |    |    |-- uid: long (nullable = true)
 |-- citizenshipList: struct (nullable = true)
 |    |-- citizenship: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- country: string (nullable = true)
 |    |    |    |-- mainEntry: boolean (nullable = true)
 |    |    |    |-- uid: long (nullable = true)
 |-- dateOfBirthList: struct (nullable = true)
 |    |-- dateOfBirthItem: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- dateOfBirth: string (nullable = true)
 |    |    |    |-- mainEntry: boolean (nullable = true)
 |    |    |    |-- uid: long (nullable = true)
 |-- firstName: string (nullable = true)
 |-- idList: struct (nullable = true)
 |    |-- id: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- expirationDate: string (nullable = true)
 |    |    |    |-- idCountry: string (nullable = true)
 |    |    |    |-- idNumber: string (nullable = true)
 |    |    |    |-- idType: string (nullable = true)
 |    |    |    |-- issueDate: string (nullable = true)
 |    |    |    |-- uid: long (nullable = true)
 |-- lastName: string (nullable = true)
 |-- nationalityList: struct (nullable = true)
 |    |-- nationality: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- country: string (nullable = true)
 |    |    |    |-- mainEntry: boolean (nullable = true)
 |    |    |    |-- uid: long (nullable = true)
 |-- placeOfBirthList: struct (nullable = true)
 |    |-- placeOfBirthItem: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- mainEntry: boolean (nullable = true)
 |    |    |    |-- placeOfBirth: string (nullable = true)
 |    |    |    |-- uid: long (nullable = true)
 |-- programList: struct (nullable = true)
 |    |-- program: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |-- remarks: string (nullable = true)
 |-- sdnType: string (nullable = true)
 |-- title: string (nullable = true)
 |-- uid: long (nullable = true)
 |-- vesselInfo: struct (nullable = true)
 |    |-- callSign: string (nullable = true)
 |    |-- grossRegisteredTonnage: long (nullable = true)
 |    |-- tonnage: long (nullable = true)
 |    |-- vesselFlag: string (nullable = true)
 |    |-- vesselOwner: string (nullable = true)
 |    |-- vesselType: string (nullable = true)
```

It is noteworthy that the OFAC list involves a complex structure with each alias (AKA) nested under the corresponding entity.

### UK

The UK Treasury consolidated list uses the following schema:

```
root
|-- AliasType: string (nullable = true)
|-- AliasTypeName: string (nullable = true)
|-- BusinessRegNumber: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- Country: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- CountryOfBirth: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- CurrentOwners: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- DateListed: timestamp (nullable = true)
|-- DateListedDay: long (nullable = true)
|-- DateListedMonth: long (nullable = true)
|-- DateListedYear: long (nullable = true)
|-- DateOfBirth: string (nullable = true)
|-- DateOfBirthId: struct (nullable = true)
|    |-- _VALUE: long (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- DayOfBirth: struct (nullable = true)
|    |-- _VALUE: long (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- EmailAddress: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- FCOId: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- FlagOfVessel: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- FullAddress: string (nullable = true)
|-- FullName: string (nullable = true)
|-- FurtherIdentifiyingInformation: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- Gender: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- GroupID: long (nullable = true)
|-- GroupStatus: string (nullable = true)
|-- GroupTypeDescription: string (nullable = true)
|-- GrpStatus: string (nullable = true)
|-- HIN: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- ID: long (nullable = true)
|-- IMONumber: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- LastUpdated: timestamp (nullable = true)
|-- LastUpdatedDay: long (nullable = true)
|-- LastUpdatedMonth: long (nullable = true)
|-- LastUpdatedYear: long (nullable = true)
|-- LengthOfVessel: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- ListingType: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- MonthOfBirth: struct (nullable = true)
|    |-- _VALUE: long (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- Name6: string (nullable = true)
|-- NameTitle: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- NationalIdNumber: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- Nationality: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- OrgType: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- OtherInformation: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- ParentCompany: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- PassportDetails: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- PhoneNumber: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- Position: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- PostCode: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- PreviousFlags: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- PreviousOwners: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- RegimeName: string (nullable = true)
|-- Subsidiaries: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- TonnageOfVessel: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- TownOfBirth: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- TypeOfVessel: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- UKStatementOfReasons: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- Website: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- YearBuilt: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- YearOfBirth: struct (nullable = true)
|    |-- _VALUE: long (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- address1: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- address2: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- address3: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- address4: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- address5: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- address6: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- name1: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- name2: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- name3: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- name4: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
|-- name5: struct (nullable = true)
|    |-- _VALUE: string (nullable = true)
|    |-- _i:nil: boolean (nullable = true)
``` 

In contrast to the US OFAC list, the UK Treasury uses a flattened schema with the entity (or "Prime Alias") on the same level as any AKA or FKA.

### Union

In consolidating the data from these two sources, the first decision that needs to be made is whether to adopt a nested structure (like the US OFAC) or a flattened one (like the UK Treasury). For simplicity of implementation, I chose the latter. This involves 'exploding' the US OFAC akaList. As a result, the final combined dataframe is a union of three sources: the UK Treasury list, the top level US OFAC list (for Prime Aliases) and the exploded US OFAC `akaList` (for AKAs, FKAs, and NKAs). The final flattened schema looks like this:

| Column | UK Treasury | US OFAC Prime | US OFAC AKA | Notes |
| :----- | :---------- | :------------ | :---------- | :---- |
| `AliasType` | `AliasType` | "Prime Alias" | `aka.type` | US OFAC `aka.type` is captalized and has `.` removed to conform with UK Treasury standard (_e.g._ `'f.k.a.'` becomes `'FKA'`)
| `ID`   | `ID` | `uid` | `aka.uid` |
| `firstName` | `name1._VALUE` | `firstName` | `aka.firstName` |
| `lastName` | `Name6` | `lastName` | `aka.lastName` |
| `GroupID` | `GroupID` | `uid` | `uid` | for OFAC Prime Alias, GroupID = uid. OFAC AKA inherits GroupID from Prime Alias
| `GroupTypeDescription` | `GroupTypeDescription` | `sdnType` | `sdnType` | OFAC AKA inherits type from Prime Alias
| `source` | "UK" | "US" | "US" |

## Limitations

### Schema

By adopting the UK-style schema, flattening aliases into their own rows, I made it effectively impossible to add other nested data, such as addresses, nationalities, and so on, which, in the OFAC list, are associated with the prime alias. This is fine, if those data are not needed. If they are, a restructuring would be in order to either:

1. Adopt the OFAC-style and nest AKAs under the Prime Alias entities.
2. Adopt a relational approach, with a table for Prime Alias entities, a table for AKAs, a table for addresses, a table for nationalities, and so on.

The second approach is a little more complicated, but provides greater long-term flexibility, and has the advantage of being backwards compatible with the existing solution (where the existing result would simply become the AKA table, and GroupID would become the foreign key to the Prime Alias table, _etc._) Since the assignment called for "a single dataset in parquet format" I consider this later normalized approach to be out of scope for this problem.

### Dependencies

The main dependency in this approach is the Databricks XML library which is referenced in ` --packages com.databricks:spark-xml_2.12:0.12.0`.

I was also unable to get my local instance of Spark to be able to write Parquet directly, and so included a hack using PyArrow. As I indicate in the docstring for the function that uses that hack, this approach is suboptimal because it requires the entire dataset to be collected into a single Pandas DataFrame. In fact, the whole dataset is less than 50 MB, so that is not significant (and, in fact, the use of Spark for such a small dataset is arguably overkill). However, this is not a good pattern. I considered it acceptable since debugging this machine to allow Spark to write Parquet seemed inessential to the spirit of the assignment. That said, it means that I have not actually been able to test the call to `spark_df.write.parquet` and can only cross my fingers and hope that it works on your machine (or that you have pyarrow already installed)!
