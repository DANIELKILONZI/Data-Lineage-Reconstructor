# Data-Lineage-Reconstructor

Data lineage utility to answer: **"Where did this data actually come from?"**

## What it does
- traces data across pipelines
- reconstructs transformations
- shows a full lineage graph (pipelines, transformations, datasets)

## Primary use cases
- fintech audits
- analytics debugging

## Quick example
```python
from lineage import DataLineageReconstructor, Transformation

reconstructor = DataLineageReconstructor([
    Transformation("normalize", ("raw_tx",), ("clean_tx",), "ingest"),
    Transformation("features", ("clean_tx",), ("risk_features",), "risk"),
])

full_graph = reconstructor.full_lineage_graph()
upstream = reconstructor.trace_data_across_pipelines("risk_features")
```
