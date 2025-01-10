r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageSwitchConnectionsSourcePort", "StorageSwitchConnectionsSourcePortSchema"]
__pdoc__ = {
    "StorageSwitchConnectionsSourcePortSchema.resource": False,
    "StorageSwitchConnectionsSourcePortSchema.opts": False,
    "StorageSwitchConnectionsSourcePort": False,
}


class StorageSwitchConnectionsSourcePortSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageSwitchConnectionsSourcePort object"""

    mode = fields.Str(data_key="mode")
    r""" Storage switch port operating mode """

    name = fields.Str(data_key="name")
    r""" Storage switch port name """

    wwn = fields.Str(data_key="wwn")
    r""" Storage switch peer port world wide name """

    @property
    def resource(self):
        return StorageSwitchConnectionsSourcePort

    gettable_fields = [
        "mode",
        "name",
        "wwn",
    ]
    """mode,name,wwn,"""

    patchable_fields = [
        "mode",
        "name",
        "wwn",
    ]
    """mode,name,wwn,"""

    postable_fields = [
        "mode",
        "name",
        "wwn",
    ]
    """mode,name,wwn,"""


class StorageSwitchConnectionsSourcePort(Resource):

    _schema = StorageSwitchConnectionsSourcePortSchema
