r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FileMoveScanner", "FileMoveScannerSchema"]
__pdoc__ = {
    "FileMoveScannerSchema.resource": False,
    "FileMoveScannerSchema.opts": False,
    "FileMoveScanner": False,
}


class FileMoveScannerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FileMoveScanner object"""

    percent = Size(data_key="percent")
    r""" Scanner progress, as a percentage.

Example: 80 """

    progress = Size(data_key="progress")
    r""" Scanner progress, in bytes scanned.

Example: 80000 """

    state = fields.Str(data_key="state")
    r""" Status of the file move scanner.

Valid choices:

* allocation_map
* complete
* data
* destroyed
* destroying
* paused_admin
* paused_error
* preparing """

    total = Size(data_key="total")
    r""" Total bytes to be scanned.

Example: 100000 """

    @property
    def resource(self):
        return FileMoveScanner

    gettable_fields = [
        "percent",
        "progress",
        "state",
        "total",
    ]
    """percent,progress,state,total,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class FileMoveScanner(Resource):

    _schema = FileMoveScannerSchema
