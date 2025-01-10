r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NfsServiceProtocolV41Features", "NfsServiceProtocolV41FeaturesSchema"]
__pdoc__ = {
    "NfsServiceProtocolV41FeaturesSchema.resource": False,
    "NfsServiceProtocolV41FeaturesSchema.opts": False,
    "NfsServiceProtocolV41Features": False,
}


class NfsServiceProtocolV41FeaturesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NfsServiceProtocolV41Features object"""

    acl_enabled = fields.Boolean(data_key="acl_enabled")
    r""" Specifies whether NFSv4.1 or later ACLs is enabled. """

    implementation_domain = fields.Str(data_key="implementation_domain")
    r""" Specifies the NFSv4.1 or later implementation ID domain. """

    implementation_name = fields.Str(data_key="implementation_name")
    r""" Specifies the NFSv4.1 or later implementation ID name. """

    pnfs_enabled = fields.Boolean(data_key="pnfs_enabled")
    r""" Specifies whether NFSv4.1 or later Parallel NFS is enabled. """

    read_delegation_enabled = fields.Boolean(data_key="read_delegation_enabled")
    r""" Specifies whether NFSv4.1 or later Read Delegation is enabled. """

    referrals_enabled = fields.Boolean(data_key="referrals_enabled")
    r""" Specifies whether NFSv4.1 referrals is enabled. """

    trunking_enabled = fields.Boolean(data_key="trunking_enabled")
    r""" Specifies whether NFSv4.1 or later trunking is enabled. """

    write_delegation_enabled = fields.Boolean(data_key="write_delegation_enabled")
    r""" Specifies whether NFSv4.1 or later Write Delegation is enabled. """

    @property
    def resource(self):
        return NfsServiceProtocolV41Features

    gettable_fields = [
        "acl_enabled",
        "implementation_domain",
        "implementation_name",
        "pnfs_enabled",
        "read_delegation_enabled",
        "referrals_enabled",
        "trunking_enabled",
        "write_delegation_enabled",
    ]
    """acl_enabled,implementation_domain,implementation_name,pnfs_enabled,read_delegation_enabled,referrals_enabled,trunking_enabled,write_delegation_enabled,"""

    patchable_fields = [
        "acl_enabled",
        "implementation_domain",
        "implementation_name",
        "pnfs_enabled",
        "read_delegation_enabled",
        "referrals_enabled",
        "trunking_enabled",
        "write_delegation_enabled",
    ]
    """acl_enabled,implementation_domain,implementation_name,pnfs_enabled,read_delegation_enabled,referrals_enabled,trunking_enabled,write_delegation_enabled,"""

    postable_fields = [
        "acl_enabled",
        "implementation_domain",
        "implementation_name",
        "pnfs_enabled",
        "read_delegation_enabled",
        "referrals_enabled",
        "trunking_enabled",
        "write_delegation_enabled",
    ]
    """acl_enabled,implementation_domain,implementation_name,pnfs_enabled,read_delegation_enabled,referrals_enabled,trunking_enabled,write_delegation_enabled,"""


class NfsServiceProtocolV41Features(Resource):

    _schema = NfsServiceProtocolV41FeaturesSchema
