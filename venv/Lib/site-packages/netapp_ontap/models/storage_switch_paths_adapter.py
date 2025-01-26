r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageSwitchPathsAdapter", "StorageSwitchPathsAdapterSchema"]
__pdoc__ = {
    "StorageSwitchPathsAdapterSchema.resource": False,
    "StorageSwitchPathsAdapterSchema.opts": False,
    "StorageSwitchPathsAdapter": False,
}


class StorageSwitchPathsAdapterSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageSwitchPathsAdapter object"""

    name = fields.Str(data_key="name")
    r""" Node adapter name """

    type = fields.Str(data_key="type")
    r""" Node adapter type

Valid choices:

* unknown
* fcp_initiator
* fc_vi
* fcp_target """

    wwn = fields.Str(data_key="wwn")
    r""" Node adapter world wide name """

    @property
    def resource(self):
        return StorageSwitchPathsAdapter

    gettable_fields = [
        "name",
        "type",
        "wwn",
    ]
    """name,type,wwn,"""

    patchable_fields = [
        "name",
        "type",
        "wwn",
    ]
    """name,type,wwn,"""

    postable_fields = [
        "name",
        "type",
        "wwn",
    ]
    """name,type,wwn,"""


class StorageSwitchPathsAdapter(Resource):

    _schema = StorageSwitchPathsAdapterSchema
