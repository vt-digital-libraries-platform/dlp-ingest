from ingest_classes.generic_type import GenericType
from ingest_classes.digital_objects.three_d_digital_object import ThreeDDigitalObject
from ingest_classes.metadata.three_d_metadata import ThreeDMetadata


class ThreeDType(GenericType):
    media_class = ThreeDDigitalObject
    metadata_class = ThreeDMetadata
