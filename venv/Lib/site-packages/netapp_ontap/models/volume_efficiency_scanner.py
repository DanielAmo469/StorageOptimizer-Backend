r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeEfficiencyScanner", "VolumeEfficiencyScannerSchema"]
__pdoc__ = {
    "VolumeEfficiencyScannerSchema.resource": False,
    "VolumeEfficiencyScannerSchema.opts": False,
    "VolumeEfficiencyScanner": False,
}


class VolumeEfficiencyScannerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeEfficiencyScanner object"""

    compression = fields.Boolean(data_key="compression")
    r""" Start compression if scanning old data. Valid for PATCH and GET. This option is not supported for FSX/CVO platforms. """

    dedupe = fields.Boolean(data_key="dedupe")
    r""" Start deduplication if scanning old data. Valid for PATCH and GET. """

    scan_old_data = fields.Boolean(data_key="scan_old_data")
    r""" Indicates whether or not to scan old data. Valid for PATCH and GET. """

    state = fields.Str(data_key="state")
    r""" State of the volume efficiency scanner. Valid for PATCH and GET. Valid options for PATCH are "idle" and "active".

Valid choices:

* idle
* initializing
* active
* undoing
* pending
* downgrading
* disabled """

    @property
    def resource(self):
        return VolumeEfficiencyScanner

    gettable_fields = [
        "compression",
        "dedupe",
        "scan_old_data",
        "state",
    ]
    """compression,dedupe,scan_old_data,state,"""

    patchable_fields = [
        "compression",
        "dedupe",
        "scan_old_data",
        "state",
    ]
    """compression,dedupe,scan_old_data,state,"""

    postable_fields = [
    ]
    """"""


class VolumeEfficiencyScanner(Resource):

    _schema = VolumeEfficiencyScannerSchema
