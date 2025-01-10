r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["TapeDeviceAliases", "TapeDeviceAliasesSchema"]
__pdoc__ = {
    "TapeDeviceAliasesSchema.resource": False,
    "TapeDeviceAliasesSchema.opts": False,
    "TapeDeviceAliases": False,
}


class TapeDeviceAliasesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TapeDeviceAliases object"""

    mapping = fields.Str(data_key="mapping")
    r""" Alias mapping.

Example: SN[10WT000933] """

    name = fields.Str(data_key="name")
    r""" Alias name.

Example: st6 """

    @property
    def resource(self):
        return TapeDeviceAliases

    gettable_fields = [
        "mapping",
        "name",
    ]
    """mapping,name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class TapeDeviceAliases(Resource):

    _schema = TapeDeviceAliasesSchema
