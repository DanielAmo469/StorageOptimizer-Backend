r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SnapshotProvenanceVolume", "SnapshotProvenanceVolumeSchema"]
__pdoc__ = {
    "SnapshotProvenanceVolumeSchema.resource": False,
    "SnapshotProvenanceVolumeSchema.opts": False,
    "SnapshotProvenanceVolume": False,
}


class SnapshotProvenanceVolumeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SnapshotProvenanceVolume object"""

    uuid = fields.Str(data_key="uuid")
    r""" UUID for the volume that is used to identify the source volume in a mirroring relationship. When the mirroring relationship is broken, a volume's Instance UUID and Provenance UUID are made identical. An unmirrored volume's Provenance UUID is the same as its Instance UUID. This field is valid for flexible volumes only.

Example: 4cd8a442-86d1-11e0-ae1c-125648563413 """

    @property
    def resource(self):
        return SnapshotProvenanceVolume

    gettable_fields = [
        "uuid",
    ]
    """uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class SnapshotProvenanceVolume(Resource):

    _schema = SnapshotProvenanceVolumeSchema
