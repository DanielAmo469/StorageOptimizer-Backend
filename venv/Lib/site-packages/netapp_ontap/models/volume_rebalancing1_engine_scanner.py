r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeRebalancing1EngineScanner", "VolumeRebalancing1EngineScannerSchema"]
__pdoc__ = {
    "VolumeRebalancing1EngineScannerSchema.resource": False,
    "VolumeRebalancing1EngineScannerSchema.opts": False,
    "VolumeRebalancing1EngineScanner": False,
}


class VolumeRebalancing1EngineScannerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeRebalancing1EngineScanner object"""

    blocks_scanned = Size(data_key="blocks_scanned")
    r""" Number of blocks scanned on this constituent. """

    blocks_skipped = fields.Nested("netapp_ontap.models.volume_rebalancing1_engine_scanner_blocks_skipped.VolumeRebalancing1EngineScannerBlocksSkippedSchema", unknown=EXCLUDE, data_key="blocks_skipped")
    r""" The blocks_skipped field of the volume_rebalancing1_engine_scanner. """

    files_scanned = Size(data_key="files_scanned")
    r""" Number of files scanned on this constituent. """

    files_skipped = fields.Nested("netapp_ontap.models.volume_rebalancing1_engine_scanner_files_skipped.VolumeRebalancing1EngineScannerFilesSkippedSchema", unknown=EXCLUDE, data_key="files_skipped")
    r""" The files_skipped field of the volume_rebalancing1_engine_scanner. """

    @property
    def resource(self):
        return VolumeRebalancing1EngineScanner

    gettable_fields = [
        "blocks_scanned",
        "blocks_skipped",
        "files_scanned",
        "files_skipped",
    ]
    """blocks_scanned,blocks_skipped,files_scanned,files_skipped,"""

    patchable_fields = [
        "blocks_skipped",
        "files_skipped",
    ]
    """blocks_skipped,files_skipped,"""

    postable_fields = [
        "blocks_skipped",
        "files_skipped",
    ]
    """blocks_skipped,files_skipped,"""


class VolumeRebalancing1EngineScanner(Resource):

    _schema = VolumeRebalancing1EngineScannerSchema
