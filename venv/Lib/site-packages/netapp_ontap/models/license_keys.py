r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LicenseKeys", "LicenseKeysSchema"]
__pdoc__ = {
    "LicenseKeysSchema.resource": False,
    "LicenseKeysSchema.opts": False,
    "LicenseKeys": False,
}


class LicenseKeysSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LicenseKeys object"""

    keys = fields.List(fields.Str, data_key="keys")
    r""" The keys field of the license_keys. """

    @property
    def resource(self):
        return LicenseKeys

    gettable_fields = [
        "keys",
    ]
    """keys,"""

    patchable_fields = [
        "keys",
    ]
    """keys,"""

    postable_fields = [
        "keys",
    ]
    """keys,"""


class LicenseKeys(Resource):

    _schema = LicenseKeysSchema
