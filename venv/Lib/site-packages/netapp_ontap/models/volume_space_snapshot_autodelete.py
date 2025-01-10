r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeSpaceSnapshotAutodelete", "VolumeSpaceSnapshotAutodeleteSchema"]
__pdoc__ = {
    "VolumeSpaceSnapshotAutodeleteSchema.resource": False,
    "VolumeSpaceSnapshotAutodeleteSchema.opts": False,
    "VolumeSpaceSnapshotAutodelete": False,
}


class VolumeSpaceSnapshotAutodeleteSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeSpaceSnapshotAutodelete object"""

    commitment = fields.Str(data_key="commitment")
    r""" By default, Snapshot copy autodelete does not delete Snapshot copies locked by Snapmirror, clones of a volume, a LUN, an NVMe namespace, or a file.  Deletion of Snapshot copies locked by these applications is specified using this option. The default value is try.

Valid choices:

* try
* disrupt
* destroy """

    defer_delete = fields.Str(data_key="defer_delete")
    r""" Allows the user to inform Snapshot copy autodelete to defer the deletion of a specified Snapshot copy until the end. The default value is user_created.

Valid choices:

* scheduled
* user_created
* prefix
* none """

    delete_order = fields.Str(data_key="delete_order")
    r""" Specifies the order in which Snapshot copy autodelete occurs. Ordering is done using the date and time the Snapshot copy is created. The default value is oldest_first.

Valid choices:

* newest_first
* oldest_first """

    enabled = fields.Boolean(data_key="enabled")
    r""" Specifies whether Snapshot copy autodelete is currently enabled on this volume. """

    prefix = fields.Str(data_key="prefix")
    r""" Specifies the prefix of the Snapshot copy which if matched, is deleted last. Used with autodelete_defer_delete when used with a prefix value. """

    target_free_space = Size(data_key="target_free_space")
    r""" Snapshot copies are deleted, one at a time, until the used volume space reaches the value specified. The default is 20% free space or 80% utilized. """

    trigger = fields.Str(data_key="trigger")
    r""" Specifies when the system should trigger an autodelete of Snapshot copies. When set to _volume_, autodelete is triggered based on volume fullness. When set to _snap_reserve_, autodelete is triggered based on Snapshot copy reserve fullness. The default value is _volume_.

Valid choices:

* volume
* snap_reserve """

    @property
    def resource(self):
        return VolumeSpaceSnapshotAutodelete

    gettable_fields = [
        "commitment",
        "defer_delete",
        "delete_order",
        "prefix",
        "target_free_space",
        "trigger",
    ]
    """commitment,defer_delete,delete_order,prefix,target_free_space,trigger,"""

    patchable_fields = [
        "commitment",
        "defer_delete",
        "delete_order",
        "enabled",
        "prefix",
        "target_free_space",
        "trigger",
    ]
    """commitment,defer_delete,delete_order,enabled,prefix,target_free_space,trigger,"""

    postable_fields = [
    ]
    """"""


class VolumeSpaceSnapshotAutodelete(Resource):

    _schema = VolumeSpaceSnapshotAutodeleteSchema
