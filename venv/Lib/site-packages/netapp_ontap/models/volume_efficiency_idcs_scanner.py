r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeEfficiencyIdcsScanner", "VolumeEfficiencyIdcsScannerSchema"]
__pdoc__ = {
    "VolumeEfficiencyIdcsScannerSchema.resource": False,
    "VolumeEfficiencyIdcsScannerSchema.opts": False,
    "VolumeEfficiencyIdcsScanner": False,
}


class VolumeEfficiencyIdcsScannerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeEfficiencyIdcsScanner object"""

    enabled = fields.Boolean(data_key="enabled")
    r""" Specifies the administrative state of the inactive data compression scanner. """

    inactive_days = Size(data_key="inactive_days")
    r""" Data blocks older than, or equal to, 'inactive_days' are picked up by the inactive data compression scanner. Valid for PATCH only. Only applicable when 'operation_state' set to 'active'. """

    mode = fields.Str(data_key="mode")
    r""" Specifies the mode of inactive data compression scanner. Valid for PATCH and GET.

Valid choices:

* default
* compute_compression_savings """

    operation_state = fields.Str(data_key="operation_state")
    r""" Specifies the operational state of the inactive data compression scanner. VALID for PATCH and GET. Valid options for PATCH are "idle" and "active".

Valid choices:

* idle
* active """

    status = fields.Str(data_key="status")
    r""" Status of last inactive data compression scan on the volume.

Valid choices:

* success
* failure """

    threshold_inactive_time = fields.Str(data_key="threshold_inactive_time")
    r""" Time interval after which inactive data compression is automatically triggered. The value is in days and is represented in the ISO-8601 format "P<num>D", for example "P3D" represents a duration of 3 days.

Example: P14D """

    @property
    def resource(self):
        return VolumeEfficiencyIdcsScanner

    gettable_fields = [
        "enabled",
        "mode",
        "operation_state",
        "status",
        "threshold_inactive_time",
    ]
    """enabled,mode,operation_state,status,threshold_inactive_time,"""

    patchable_fields = [
        "inactive_days",
        "mode",
        "operation_state",
    ]
    """inactive_days,mode,operation_state,"""

    postable_fields = [
        "mode",
        "operation_state",
    ]
    """mode,operation_state,"""


class VolumeEfficiencyIdcsScanner(Resource):

    _schema = VolumeEfficiencyIdcsScannerSchema
