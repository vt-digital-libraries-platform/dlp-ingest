from lib_files.media_types.iiif_type import IIIFType
from lib_files.media_types.three_d_type import ThreeDType

media_types_map = {
  "iiif": {
    "extensions": ["json"],
    "handler": IIIFType
  },
  "3d": {
    "extensions": ["x3d","obj"],
    "handler": ThreeDType
  }
}