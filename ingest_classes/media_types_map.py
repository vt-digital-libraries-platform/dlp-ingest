import os
from ingest_classes.iiif_type import IIIFType
from ingest_classes.three_d_type import ThreeDType
from ingest_classes.pdf_type import PDFType

# collection asset paths are relative to the collection root directory
# item asset paths are relative to the item subdirectories
media_types_map = {
    "iiif": {
        "assets": {
            "collection": {
                "thumbnail": "representative.jpg",
                "collection_metadata": "<variable>_collection_metadata.csv",
                "item_metadata": "<variable>_archive_metadata.csv"
            },
            "item": {},
        },
        "extensions": ["json"],
        "handler": IIIFType,
    },
    "pdf": {
        "assets": {
            "collection": {
                "thumbnail": "representative.jpg",
                "collection_metadata": "<variable>_collection_metadata.csv",
                "item_metadata": "<variable>_archive_metadata.csv"
            },
            "item": {
                "pdf": "<item_identifier>.pdf",
                "thumbnail": "<item_identifier>_thumbnail.jpg",
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
                "collection_metadata": "<variable>_collection_metadata.csv",
                "item_metadata": "<variable>_archive_metadata.csv"
            },
            "item": {
                "iiif_manifest": "manifest.json",
                "gltf_config": "3D/GLB/<item_identifier>.glb",
                "env_config": "studio.env",
                "thumbnail": "<item_identifier>_thumbnail.jpg",
            },
        },
        "extensions": ["glb", "gltf"],
        "handler": ThreeDType,
    },
    "3d": {
        "assets": {
            "collection": {
                "thumbnail": "representative.jpg",
                "collection_metadata": "<variable>_collection_metadata.csv",
                "item_metadata": "<variable>_archive_metadata.csv"
            },
            "item": {
                "gltf_config": "3D/GLB/<item_identifier>.glb",
                "env_config": "studio.env",
                "thumbnail": "<item_identifier>_thumbnail.jpg",
            },
        },
        "extensions": ["glb", "gltf"],
        "handler": ThreeDType,
    }
}
