r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupCifsShare", "ConsistencyGroupCifsShareSchema"]
__pdoc__ = {
    "ConsistencyGroupCifsShareSchema.resource": False,
    "ConsistencyGroupCifsShareSchema.opts": False,
    "ConsistencyGroupCifsShare": False,
}


class ConsistencyGroupCifsShareSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupCifsShare object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the consistency_group_cifs_share. """

    access_based_enumeration = fields.Boolean(data_key="access_based_enumeration")
    r""" Specifies whether all folders inside this share are visible to a user based on that individual user's access right; prevents
the display of folders or other shared resources that the user does not have access to. """

    acls = fields.List(fields.Nested("netapp_ontap.models.cifs_share_acls.CifsShareAclsSchema", unknown=EXCLUDE), data_key="acls")
    r""" The acls field of the consistency_group_cifs_share. """

    allow_unencrypted_access = fields.Boolean(data_key="allow_unencrypted_access")
    r""" Specifies whether or not the SMB2 clients are allowed to access the encrypted share. """

    change_notify = fields.Boolean(data_key="change_notify")
    r""" Specifies whether CIFS clients can request for change notifications for directories on this share. """

    comment = fields.Str(data_key="comment")
    r""" Specify the CIFS share descriptions.

Example: HR Department Share """

    continuously_available = fields.Boolean(data_key="continuously_available")
    r""" Specifies whether or not the clients connecting to this share can open files in a persistent manner.
Files opened in this way are protected from disruptive events, such as, failover and giveback. """

    dir_umask = Size(data_key="dir_umask")
    r""" Directory mode creation mask to be viewed as an octal number.

Example: 18 """

    encryption = fields.Boolean(data_key="encryption")
    r""" Specifies whether SMB encryption must be used when accessing this share. Clients that do not support encryption are not
able to access this share. """

    file_umask = Size(data_key="file_umask")
    r""" File mode creation mask to be viewed as an octal number.

Example: 18 """

    home_directory = fields.Boolean(data_key="home_directory")
    r""" Specifies whether or not the share is a home directory share, where the share and path names are dynamic.
ONTAP home directory functionality automatically offer each user a dynamic share to their home directory without creating an
individual SMB share for each user.
The ONTAP CIFS home directory feature enable us to configure a share that maps to
different directories based on the user that connects to it. Instead of creating a separate shares for each user,
a single share with a home directory parameters can be created.
In a home directory share, ONTAP dynamically generates the share-name and share-path by substituting
%w, %u, and %d variables with the corresponding Windows user name, UNIX user name, and domain name, respectively. """

    name = fields.Str(data_key="name")
    r""" Specifies the name of the CIFS share that you want to create. If this
is a home directory share then the share name includes the pattern as
%w (Windows user name), %u (UNIX user name) and %d (Windows domain name)
variables in any combination with this parameter to generate shares dynamically.


Example: HR_SHARE """

    namespace_caching = fields.Boolean(data_key="namespace_caching")
    r""" Specifies whether or not the SMB clients connecting to this share can cache the directory enumeration
results returned by the CIFS servers. """

    no_strict_security = fields.Boolean(data_key="no_strict_security")
    r""" Specifies whether or not CIFS clients can follow Unix symlinks outside the share boundaries. """

    offline_files = fields.Str(data_key="offline_files")
    r""" Offline Files
The supported values are:

  * none - Clients are not permitted to cache files for offline access.
  * manual - Clients may cache files that are explicitly selected by the user for offline access.
  * documents - Clients may automatically cache files that are used by the user for offline access.
  * programs - Clients may automatically cache files that are used by the user for offline access
               and may use those files in an offline mode even if the share is available.


Valid choices:

* none
* manual
* documents
* programs """

    oplocks = fields.Boolean(data_key="oplocks")
    r""" Specifies whether opportunistic locks are enabled on this share. "Oplocks" allow clients to lock files and cache content locally,
which can increase performance for file operations. """

    show_snapshot = fields.Boolean(data_key="show_snapshot")
    r""" Specifies whether or not the Snapshot copies can be viewed and traversed by clients. """

    unix_symlink = fields.Str(data_key="unix_symlink")
    r""" Controls the access of UNIX symbolic links to CIFS clients.
The supported values are:

    * local - Enables only local symbolic links which is within the same CIFS share.
    * widelink - Enables both local symlinks and widelinks.
    * disable - Disables local symlinks and widelinks.


Valid choices:

* local
* widelink
* disable """

    vscan_profile = fields.Str(data_key="vscan_profile")
    r""" Vscan File-Operations Profile
The supported values are:

  * no_scan - Virus scans are never triggered for accesses to this share.
  * standard - Virus scans can be triggered by open, close, and rename operations.
  * strict - Virus scans can be triggered by open, read, close, and rename operations.
  * writes_only - Virus scans can be triggered only when a file that has been modified is closed.


Valid choices:

* no_scan
* standard
* strict
* writes_only """

    @property
    def resource(self):
        return ConsistencyGroupCifsShare

    gettable_fields = [
        "links",
        "access_based_enumeration",
        "acls",
        "allow_unencrypted_access",
        "change_notify",
        "comment",
        "continuously_available",
        "dir_umask",
        "encryption",
        "file_umask",
        "home_directory",
        "name",
        "namespace_caching",
        "no_strict_security",
        "offline_files",
        "oplocks",
        "show_snapshot",
        "unix_symlink",
        "vscan_profile",
    ]
    """links,access_based_enumeration,acls,allow_unencrypted_access,change_notify,comment,continuously_available,dir_umask,encryption,file_umask,home_directory,name,namespace_caching,no_strict_security,offline_files,oplocks,show_snapshot,unix_symlink,vscan_profile,"""

    patchable_fields = [
        "access_based_enumeration",
        "allow_unencrypted_access",
        "change_notify",
        "comment",
        "continuously_available",
        "dir_umask",
        "encryption",
        "file_umask",
        "namespace_caching",
        "no_strict_security",
        "offline_files",
        "oplocks",
        "show_snapshot",
        "unix_symlink",
        "vscan_profile",
    ]
    """access_based_enumeration,allow_unencrypted_access,change_notify,comment,continuously_available,dir_umask,encryption,file_umask,namespace_caching,no_strict_security,offline_files,oplocks,show_snapshot,unix_symlink,vscan_profile,"""

    postable_fields = [
        "access_based_enumeration",
        "acls",
        "allow_unencrypted_access",
        "change_notify",
        "comment",
        "continuously_available",
        "dir_umask",
        "encryption",
        "file_umask",
        "home_directory",
        "name",
        "namespace_caching",
        "no_strict_security",
        "offline_files",
        "oplocks",
        "show_snapshot",
        "unix_symlink",
        "vscan_profile",
    ]
    """access_based_enumeration,acls,allow_unencrypted_access,change_notify,comment,continuously_available,dir_umask,encryption,file_umask,home_directory,name,namespace_caching,no_strict_security,offline_files,oplocks,show_snapshot,unix_symlink,vscan_profile,"""


class ConsistencyGroupCifsShare(Resource):

    _schema = ConsistencyGroupCifsShareSchema
