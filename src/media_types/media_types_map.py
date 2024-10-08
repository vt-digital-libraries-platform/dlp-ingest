from src.media_types.iiif_type import IIIFType
from src.media_types.three_d_type import ThreeDType
from src.media_types.pdf_type import PDFType

# collection asset paths are relative to the collection root directory
# item asset paths are relative to the item subdirectories
media_types_map = {
    "iiif": {
        "assets": {
            "collection": {},
            "item": {},
        },
        "extensions": ["json"],
        "handler": IIIFType,
    },
    "pdf": {
        "assets": {
            "collection": {
                "thumbnail": "representative.jpg",
                "metadata": [
                    "<variable>_collection_metadata.csv",
                    "<variable>_archive_metadata.csv",
                ],
            },
            "item": {
                "pdf": "<variable>.pdf",
                "thumbnail": "<variable>thumbnail.jpg",
            },
            "options": {"asset_src": "pdf"},
        },
        "extensions": ["pdf", "jpg"],
        "handler": PDFType,
    },
    "3d_2diiif": {
        "assets": {
            "collection": {
                "thumbnail": "representative.jpg",
                "metadata": [
                    "<variable>_collection_metadata.csv",
                    "<variable>_archive_metadata.csv",
                ],
            },
            "item": {
                "iiif_manifest": "manifest.json",
                "x3d_config": "3D/X3D/LowRes/LowRes_<item_identifier>_X3D.x3d",
                "x3d_src_img": "3D/X3D/LowRes/LowRes_<item_identifier>_X3D.png",
                "morpho_thumb": "Morphosource Thumbnails/HighRes_<item_identifier>_thumbnail.png",
            },
        },
        "extensions": ["x3d"],
        "handler": ThreeDType,
    },
}
