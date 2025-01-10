r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NfsServiceSecurity", "NfsServiceSecuritySchema"]
__pdoc__ = {
    "NfsServiceSecuritySchema.resource": False,
    "NfsServiceSecuritySchema.opts": False,
    "NfsServiceSecurity": False,
}


class NfsServiceSecuritySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NfsServiceSecurity object"""

    chown_mode = fields.Str(data_key="chown_mode")
    r""" Specifies whether file ownership can be changed only by the superuser, or if a non-root user can also change file ownership. If you set this parameter to restricted, file ownership can be changed only by the superuser, even though the on-disk permissions allow a non-root user to change file ownership. If you set this parameter to unrestricted, file ownership can be changed by the superuser and by the non-root user, depending upon the access granted by on-disk permissions. If you set this parameter to use-export-policy, file ownership can be changed in accordance with the relevant export rules.

Valid choices:

* restricted
* unrestricted
* use_export_policy """

    nt_acl_display_permission = fields.Boolean(data_key="nt_acl_display_permission")
    r""" Controls the permissions that are displayed to NFSv3 and NFSv4 clients on a file or directory that has an NT ACL set. When true, the displayed permissions are based on the maximum access granted by the NT ACL to any user. When false, the displayed permissions are based on the minimum access granted by the NT ACL to any user. """

    ntfs_unix_security = fields.Str(data_key="ntfs_unix_security")
    r""" Specifies how NFSv3 security changes affect NTFS volumes. If you set this parameter to ignore, ONTAP ignores NFSv3 security changes. If you set this parameter to fail, this overrides the UNIX security options set in the relevant export rules. If you set this parameter to use_export_policy, ONTAP processes NFSv3 security changes in accordance with the relevant export rules.

Valid choices:

* ignore
* fail
* use_export_policy """

    permitted_encryption_types = fields.List(fields.Str, data_key="permitted_encryption_types")
    r""" Specifies the permitted encryption types for Kerberos over NFS. """

    rpcsec_context_idle = Size(data_key="rpcsec_context_idle")
    r""" Specifies, in seconds, the amount of time a RPCSEC_GSS context is permitted to remain unused before it is deleted. """

    @property
    def resource(self):
        return NfsServiceSecurity

    gettable_fields = [
        "chown_mode",
        "nt_acl_display_permission",
        "ntfs_unix_security",
        "permitted_encryption_types",
        "rpcsec_context_idle",
    ]
    """chown_mode,nt_acl_display_permission,ntfs_unix_security,permitted_encryption_types,rpcsec_context_idle,"""

    patchable_fields = [
        "chown_mode",
        "nt_acl_display_permission",
        "ntfs_unix_security",
        "permitted_encryption_types",
        "rpcsec_context_idle",
    ]
    """chown_mode,nt_acl_display_permission,ntfs_unix_security,permitted_encryption_types,rpcsec_context_idle,"""

    postable_fields = [
        "chown_mode",
        "nt_acl_display_permission",
        "ntfs_unix_security",
        "permitted_encryption_types",
        "rpcsec_context_idle",
    ]
    """chown_mode,nt_acl_display_permission,ntfs_unix_security,permitted_encryption_types,rpcsec_context_idle,"""


class NfsServiceSecurity(Resource):

    _schema = NfsServiceSecuritySchema
