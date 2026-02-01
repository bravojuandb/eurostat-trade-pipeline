# Raw data contract

**Source**: [Eurostat COMEXT dissemination service](https://ec.europa.eu/eurostat/databrowser/bulk?lang=en&selectedTab=fileComext&breadcrumbFilter=COMEXT_DATA%2FPRODUCTS)

**Purpose**: Raw landing zone (Bronze). Files are stored exactly as provided by the source.

**Format**: Bulk files (e.g. `.7z`, `.dat`) or source-native format:
```
data/raw/
    comext__snapshot_id(YYYYMMDDTHHMMSS)/
        YYYY-MM/
            ├── full_YYYYMM.dat
            └── full_YYYYMM.7z
```

**Immutability**:
- Raw data is immutable
- Files must not be modified or overwritten

**Schema guarantees**:
- Schema is defined and documented by the source (Eurostat COMEXT)
- But the pipeline does not assume schema stability.
- Downstream processing validates expected fields and types.

**Notes**:
- Raw data may be temporary or lifecycle-managed due to size and cost
- Downstream processing must not rely on raw schema stability
