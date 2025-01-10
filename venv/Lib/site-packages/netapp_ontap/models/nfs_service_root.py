r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NfsServiceRoot", "NfsServiceRootSchema"]
__pdoc__ = {
    "NfsServiceRootSchema.resource": False,
    "NfsServiceRootSchema.opts": False,
    "NfsServiceRoot": False,
}


class NfsServiceRootSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NfsServiceRoot object"""

    ignore_nt_acl = fields.Boolean(data_key="ignore_nt_acl")
    r""" Specifies whether Windows ACLs affect root access from NFS. If this option is enabled, root access from NFS ignores the NT ACL set on the file or directory. """

    skip_write_permission_check = fields.Boolean(data_key="skip_write_permission_check")
    r""" Specifies if permission checks are to be skipped for NFS WRITE calls from root/owner. For copying read-only files to a destination folder which has inheritable ACLs, this option must be enabled. """

    @property
    def resource(self):
        return NfsServiceRoot

    gettable_fields = [
        "ignore_nt_acl",
        "skip_write_permission_check",
    ]
    """ignore_nt_acl,skip_write_permission_check,"""

    patchable_fields = [
        "ignore_nt_acl",
        "skip_write_permission_check",
    ]
    """ignore_nt_acl,skip_write_permission_check,"""

    postable_fields = [
        "ignore_nt_acl",
        "skip_write_permission_check",
    ]
    """ignore_nt_acl,skip_write_permission_check,"""


class NfsServiceRoot(Resource):

    _schema = NfsServiceRootSchema
