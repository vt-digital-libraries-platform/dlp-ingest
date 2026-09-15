import json
import urllib.request


class IIIFManifestMixin:
    """Shared by any Metadata class that needs to read a thumbnail out of a IIIF manifest.json."""

    def get_thumbnail_path_for_iiif(self, archive_dict):
        try:
            json_url = urllib.request.urlopen(archive_dict["manifest_url"])
            if json_url:
                return json.loads(json_url.read())["thumbnail"]["@id"]
        except Exception as e:
            self.logger.error(f"Error fetching thumbnail for IIIF archive {archive_dict['identifier']}: {str(e)}")
            return None
