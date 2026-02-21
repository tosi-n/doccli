import enum

__all__ = [
    "BankStatementDocumentStatus",
    "BankStatementConversionService",
    "BankAccountStatementStatus",
]


class BankAccountStatementStatus(str, enum.Enum):
    UPLOADED = "UPLOADED"
    EXTRACTED = "EXTRACTED"
    FAILED = "FAILED"
    EXTRACTING = "EXTRACTING"
    CONFIRMED = "CONFIRMED"


class BankStatementDocumentStatus(str, enum.Enum):
    UPLOADED = "UPLOADED"
    CONVERTED = "CONVERTED"
    FAILED = "FAILED"
    PROCESSING = "PROCESSING"


class BankStatementConversionService(str, enum.Enum):
    DOCUCLIPPER = "docuclipper"
    AZURE_DOCUMENT_INTELLIGENCE = "azure_document_intelligence"
