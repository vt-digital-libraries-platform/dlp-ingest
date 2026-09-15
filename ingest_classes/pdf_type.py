from ingest_classes.generic_type import GenericType
from ingest_classes.digital_objects.pdf_digital_object import PDFDigitalObject
from ingest_classes.metadata.pdf_metadata import PDFMetadata


class PDFType(GenericType):
    media_class = PDFDigitalObject
    metadata_class = PDFMetadata
