from dataclasses import dataclass

@dataclass
class DataflowSpec:
    """A schema to hold a dataflow spec used for writing to the bronze layer."""

    dataFlowId: str
    dataFlowGroup: str
    sourceFormat: str
    sourceDetails: map
    readerConfigOptions: map
    targetFormat: str
    targetDetails: map
    writerConfigOptions: map
    schema: str
    partitionColumns: list
    cdcApplyChanges: str