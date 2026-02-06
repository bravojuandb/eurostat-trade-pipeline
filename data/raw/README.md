# Raw data contract

**Source**: [Eurostat COMEXT dissemination service](https://ec.europa.eu/eurostat/databrowser/bulk?lang=en&selectedTab=fileComext&breadcrumbFilter=COMEXT_DATA%2FPRODUCTS)

**Purpose**: Raw landing zone (Bronze). Files are stored exactly as provided by the source.

**Content overview**

Each monthly dataset contains aggregated international trade records from Eurostat COMEXT, keyed by:

- REPORTER country and PARTNER country  
- multiple PRODUCT classifications (NC, SITC, CPA, BEC, section)  
- trade FLOW and statistical procedure  
- time PERIOD (monthly)

Measures include trade value (EUR and national currency) and quantities (kg and supplementary units).

Each row represents trade for a specific product between a reporter–partner pair within a single month.

**Format**: Bulk files (e.g. `.7z`, `.dat`) or source-native format:
```
data/raw/
    comext_products/
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
