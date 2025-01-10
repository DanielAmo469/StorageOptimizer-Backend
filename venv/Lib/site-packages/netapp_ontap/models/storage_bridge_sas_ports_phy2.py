r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageBridgeSasPortsPhy2", "StorageBridgeSasPortsPhy2Schema"]
__pdoc__ = {
    "StorageBridgeSasPortsPhy2Schema.resource": False,
    "StorageBridgeSasPortsPhy2Schema.opts": False,
    "StorageBridgeSasPortsPhy2": False,
}


class StorageBridgeSasPortsPhy2Schema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageBridgeSasPortsPhy2 object"""

    state = fields.Str(data_key="state")
    r""" Bridge SAS port PHY2 state """

    @property
    def resource(self):
        return StorageBridgeSasPortsPhy2

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


class StorageBridgeSasPortsPhy2(Resource):

    _schema = StorageBridgeSasPortsPhy2Schema
