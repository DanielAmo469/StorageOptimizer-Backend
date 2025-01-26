r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NfsServiceProtocolV42Features", "NfsServiceProtocolV42FeaturesSchema"]
__pdoc__ = {
    "NfsServiceProtocolV42FeaturesSchema.resource": False,
    "NfsServiceProtocolV42FeaturesSchema.opts": False,
    "NfsServiceProtocolV42Features": False,
}


class NfsServiceProtocolV42FeaturesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NfsServiceProtocolV42Features object"""

    seclabel_enabled = fields.Boolean(data_key="seclabel_enabled")
    r""" Specifies whether NFSv4.2 or later security label is enabled. """

    sparsefile_ops_enabled = fields.Boolean(data_key="sparsefile_ops_enabled")
    r""" Specifies whether NFSv4.2 or later sparsefile operation is enabled. """

    xattrs_enabled = fields.Boolean(data_key="xattrs_enabled")
    r""" Specifies whether NFSv4.2 or later extended attributes is enabled. """

    @property
    def resource(self):
        return NfsServiceProtocolV42Features

    gettable_fields = [
        "seclabel_enabled",
        "sparsefile_ops_enabled",
        "xattrs_enabled",
    ]
    """seclabel_enabled,sparsefile_ops_enabled,xattrs_enabled,"""

    patchable_fields = [
        "seclabel_enabled",
        "sparsefile_ops_enabled",
        "xattrs_enabled",
    ]
    """seclabel_enabled,sparsefile_ops_enabled,xattrs_enabled,"""

    postable_fields = [
        "seclabel_enabled",
        "sparsefile_ops_enabled",
        "xattrs_enabled",
    ]
    """seclabel_enabled,sparsefile_ops_enabled,xattrs_enabled,"""


class NfsServiceProtocolV42Features(Resource):

    _schema = NfsServiceProtocolV42FeaturesSchema
