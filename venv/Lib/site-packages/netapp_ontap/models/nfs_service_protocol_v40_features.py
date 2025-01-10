r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NfsServiceProtocolV40Features", "NfsServiceProtocolV40FeaturesSchema"]
__pdoc__ = {
    "NfsServiceProtocolV40FeaturesSchema.resource": False,
    "NfsServiceProtocolV40FeaturesSchema.opts": False,
    "NfsServiceProtocolV40Features": False,
}


class NfsServiceProtocolV40FeaturesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NfsServiceProtocolV40Features object"""

    acl_enabled = fields.Boolean(data_key="acl_enabled")
    r""" Specifies whether NFSv4.0 ACLs is enabled. """

    acl_max_aces = Size(data_key="acl_max_aces")
    r""" Specifies the maximum number of aces in a NFSv4.0 ACL.

Example: 500 """

    acl_preserve = fields.Boolean(data_key="acl_preserve")
    r""" Specifies if the NFSv4 ACL is preserved or dropped when chmod is performed. In unified security style, this parameter also specifies if NTFS file permissions are preserved or dropped when chmod, chgrp, or chown are performed. """

    read_delegation_enabled = fields.Boolean(data_key="read_delegation_enabled")
    r""" Specifies whether NFSv4.0 Read Delegation is enabled. """

    referrals_enabled = fields.Boolean(data_key="referrals_enabled")
    r""" Specifies whether NFSv4.0 referrals is enabled. """

    write_delegation_enabled = fields.Boolean(data_key="write_delegation_enabled")
    r""" Specifies whether NFSv4.0 Write Delegation is enabled. """

    @property
    def resource(self):
        return NfsServiceProtocolV40Features

    gettable_fields = [
        "acl_enabled",
        "acl_max_aces",
        "acl_preserve",
        "read_delegation_enabled",
        "referrals_enabled",
        "write_delegation_enabled",
    ]
    """acl_enabled,acl_max_aces,acl_preserve,read_delegation_enabled,referrals_enabled,write_delegation_enabled,"""

    patchable_fields = [
        "acl_enabled",
        "acl_max_aces",
        "acl_preserve",
        "read_delegation_enabled",
        "referrals_enabled",
        "write_delegation_enabled",
    ]
    """acl_enabled,acl_max_aces,acl_preserve,read_delegation_enabled,referrals_enabled,write_delegation_enabled,"""

    postable_fields = [
        "acl_enabled",
        "acl_max_aces",
        "acl_preserve",
        "read_delegation_enabled",
        "referrals_enabled",
        "write_delegation_enabled",
    ]
    """acl_enabled,acl_max_aces,acl_preserve,read_delegation_enabled,referrals_enabled,write_delegation_enabled,"""


class NfsServiceProtocolV40Features(Resource):

    _schema = NfsServiceProtocolV40FeaturesSchema
