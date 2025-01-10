r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeSpaceLogicalSpace", "VolumeSpaceLogicalSpaceSchema"]
__pdoc__ = {
    "VolumeSpaceLogicalSpaceSchema.resource": False,
    "VolumeSpaceLogicalSpaceSchema.opts": False,
    "VolumeSpaceLogicalSpace": False,
}


class VolumeSpaceLogicalSpaceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeSpaceLogicalSpace object"""

    available = Size(data_key="available")
    r""" The amount of space available in this volume with storage efficiency space considered used, in bytes. """

    enforcement = fields.Boolean(data_key="enforcement")
    r""" Specifies whether space accounting for operations on the volume is done along with storage efficiency. """

    reporting = fields.Boolean(data_key="reporting")
    r""" Specifies whether space reporting on the volume is done along with storage efficiency. """

    used = Size(data_key="used")
    r""" SUM of (physical-used, shared_refs, compression_saved_in_plane0, vbn_zero, future_blk_cnt), in bytes. """

    used_by_afs = Size(data_key="used_by_afs")
    r""" The virtual space used by AFS alone (includes volume reserves) and along with storage efficiency, in bytes. """

    used_by_snapshots = Size(data_key="used_by_snapshots")
    r""" Size that is logically used across all Snapshot copies in the volume, in bytes. """

    used_percent = Size(data_key="used_percent")
    r""" SUM of (physical-used, shared_refs, compression_saved_in_plane0, vbn_zero, future_blk_cnt), as a percentage. """

    @property
    def resource(self):
        return VolumeSpaceLogicalSpace

    gettable_fields = [
        "available",
        "enforcement",
        "reporting",
        "used",
        "used_by_afs",
        "used_by_snapshots",
        "used_percent",
    ]
    """available,enforcement,reporting,used,used_by_afs,used_by_snapshots,used_percent,"""

    patchable_fields = [
        "enforcement",
        "reporting",
    ]
    """enforcement,reporting,"""

    postable_fields = [
        "enforcement",
        "reporting",
    ]
    """enforcement,reporting,"""


class VolumeSpaceLogicalSpace(Resource):

    _schema = VolumeSpaceLogicalSpaceSchema
