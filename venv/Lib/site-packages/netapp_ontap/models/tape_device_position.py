r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["TapeDevicePosition", "TapeDevicePositionSchema"]
__pdoc__ = {
    "TapeDevicePositionSchema.resource": False,
    "TapeDevicePositionSchema.opts": False,
    "TapeDevicePosition": False,
}


class TapeDevicePositionSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TapeDevicePosition object"""

    count = Size(data_key="count")
    r""" Number of times to run position operation.

Example: 5 """

    operation = fields.Str(data_key="operation")
    r""" Position operation.

Valid choices:

* weof
* fsf
* bsf
* fsr
* bsr
* rewind
* erase
* eom """

    @property
    def resource(self):
        return TapeDevicePosition

    gettable_fields = [
        "count",
        "operation",
    ]
    """count,operation,"""

    patchable_fields = [
        "count",
        "operation",
    ]
    """count,operation,"""

    postable_fields = [
        "count",
        "operation",
    ]
    """count,operation,"""


class TapeDevicePosition(Resource):

    _schema = TapeDevicePositionSchema
