from media_type import MediaType

class IIIFType(MediaType):
  def __init__(self, data):
    super().__init__(data)
    self.data = data

iiif = IIIFType("iiif")
iiif.ingest()