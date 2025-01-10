r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeConstituentsSpaceSnapshot", "VolumeConstituentsSpaceSnapshotSchema"]
__pdoc__ = {
    "VolumeConstituentsSpaceSnapshotSchema.resource": False,
    "VolumeConstituentsSpaceSnapshotSchema.opts": False,
    "VolumeConstituentsSpaceSnapshot": False,
}


class VolumeConstituentsSpaceSnapshotSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeConstituentsSpaceSnapshot object"""

    autodelete_enabled = fields.Boolean(data_key="autodelete_enabled")
    r""" Specifies whether Snapshot copy autodelete is currently enabled on this volume. """

    reserve_percent = Size(data_key="reserve_percent")
    r""" The space that has been set aside as a reserve for Snapshot copy usage, in percent. """

    used = Size(data_key="used")
    r""" The total space used by Snapshot copies in the volume, in bytes. """

    @property
    def resource(self):
        return VolumeConstituentsSpaceSnapshot

    gettable_fields = [
        "reserve_percent",
        "used",
    ]
    """reserve_percent,used,"""

    patchable_fields = [
        "autodelete_enabled",
        "reserve_percent",
    ]
    """autodelete_enabled,reserve_percent,"""

    postable_fields = [
        "reserve_percent",
    ]
    """reserve_percent,"""


class VolumeConstituentsSpaceSnapshot(Resource):

    _schema = VolumeConstituentsSpaceSnapshotSchema
