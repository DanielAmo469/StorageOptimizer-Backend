r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LicenseManagerUri", "LicenseManagerUriSchema"]
__pdoc__ = {
    "LicenseManagerUriSchema.resource": False,
    "LicenseManagerUriSchema.opts": False,
    "LicenseManagerUri": False,
}


class LicenseManagerUriSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LicenseManagerUri object"""

    host = fields.Str(data_key="host")
    r""" License manager host name, IPv4 or IPv6 address.

Example: 10.1.1.1 """

    @property
    def resource(self):
        return LicenseManagerUri

    gettable_fields = [
        "host",
    ]
    """host,"""

    patchable_fields = [
        "host",
    ]
    """host,"""

    postable_fields = [
    ]
    """"""


class LicenseManagerUri(Resource):

    _schema = LicenseManagerUriSchema
