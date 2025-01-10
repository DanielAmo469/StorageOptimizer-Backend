r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageBridgeSasPortsPhy1", "StorageBridgeSasPortsPhy1Schema"]
__pdoc__ = {
    "StorageBridgeSasPortsPhy1Schema.resource": False,
    "StorageBridgeSasPortsPhy1Schema.opts": False,
    "StorageBridgeSasPortsPhy1": False,
}


class StorageBridgeSasPortsPhy1Schema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageBridgeSasPortsPhy1 object"""

    state = fields.Str(data_key="state")
    r""" Bridge SAS port PHY1 state """

    @property
    def resource(self):
        return StorageBridgeSasPortsPhy1

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


class StorageBridgeSasPortsPhy1(Resource):

    _schema = StorageBridgeSasPortsPhy1Schema
