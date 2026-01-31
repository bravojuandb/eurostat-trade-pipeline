# Raw data contract

**Source**: Eurostat COMEXT dissemination service

**Purpose**: Raw landing zone (Bronze). Files are stored exactly as provided by the source.

**Format**: Bulk files (e.g. `.7z`, `.dat`) or source-native format:
```
data/raw/comext_products/YYYY-MM/                                                                                 
    ├── full_YYYYMM.dat                                                                                               
    └── full_YYYYMM.7z   
```

**Immutability**:
- Raw data is immutable
- Files must not be modified or overwritten

**Schema guarantees**:
- No schema guarantees are assumed
- Schema stability depends entirely on the source

**Notes**:
- Raw data may be temporary or lifecycle-managed due to size and cost
- Downstream processing must not rely on raw schema stability
