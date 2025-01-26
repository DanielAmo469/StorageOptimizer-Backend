r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageBridgeSasPortsPhy3", "StorageBridgeSasPortsPhy3Schema"]
__pdoc__ = {
    "StorageBridgeSasPortsPhy3Schema.resource": False,
    "StorageBridgeSasPortsPhy3Schema.opts": False,
    "StorageBridgeSasPortsPhy3": False,
}


class StorageBridgeSasPortsPhy3Schema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageBridgeSasPortsPhy3 object"""

    state = fields.Str(data_key="state")
    r""" Bridge SAS port PHY3 state """

    @property
    def resource(self):
        return StorageBridgeSasPortsPhy3

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


class StorageBridgeSasPortsPhy3(Resource):

    _schema = StorageBridgeSasPortsPhy3Schema
