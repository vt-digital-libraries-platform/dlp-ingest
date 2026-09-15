from ingest_classes.generic_type import GenericType
from ingest_classes.digital_objects.iiif_digital_object import IIIFDigitalObject
from ingest_classes.metadata.iiif_metadata import IIIFMetadata


class IIIFType(GenericType):
    media_class = IIIFDigitalObject
    metadata_class = IIIFMetadata
