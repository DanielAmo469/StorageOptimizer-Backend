r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageBridgeSasPortsPhy4", "StorageBridgeSasPortsPhy4Schema"]
__pdoc__ = {
    "StorageBridgeSasPortsPhy4Schema.resource": False,
    "StorageBridgeSasPortsPhy4Schema.opts": False,
    "StorageBridgeSasPortsPhy4": False,
}


class StorageBridgeSasPortsPhy4Schema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageBridgeSasPortsPhy4 object"""

    state = fields.Str(data_key="state")
    r""" Bridge SAS port PHY4 state """

    @property
    def resource(self):
        return StorageBridgeSasPortsPhy4

    gettable_fields = [
        "state",
    ]
    """state,"""

    patchable_fields = [
        "state",
    ]
    """state,"""

    postable_fields = [
        "state",
    ]
    """state,"""


class StorageBridgeSasPortsPhy4(Resource):

    _schema = StorageBridgeSasPortsPhy4Schema
