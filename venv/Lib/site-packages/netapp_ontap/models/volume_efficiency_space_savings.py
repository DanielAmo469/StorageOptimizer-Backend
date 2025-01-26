r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeEfficiencySpaceSavings", "VolumeEfficiencySpaceSavingsSchema"]
__pdoc__ = {
    "VolumeEfficiencySpaceSavingsSchema.resource": False,
    "VolumeEfficiencySpaceSavingsSchema.opts": False,
    "VolumeEfficiencySpaceSavings": False,
}


class VolumeEfficiencySpaceSavingsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeEfficiencySpaceSavings object"""

    compression = Size(data_key="compression")
    r""" Total disk space that is saved by compressing blocks on the referenced file system, in bytes. """

    compression_percent = Size(data_key="compression_percent")
    r""" Percentage of total disk space that is saved by compressing blocks on the referenced file system. """

    dedupe = Size(data_key="dedupe")
    r""" Total disk space that is saved by deduplication and file cloning, in bytes. """

    dedupe_percent = Size(data_key="dedupe_percent")
    r""" Percentage of total disk space that is saved by deduplication and file cloning. """

    dedupe_sharing = Size(data_key="dedupe_sharing")
    r""" Total disk space that is shared due to deduplication and file cloning. """

    total = Size(data_key="total")
    r""" Total disk space saved in the volume due to deduplication, compression and file cloning, in bytes. """

    total_percent = Size(data_key="total_percent")
    r""" Percentage of total disk space saved in the volume due to deduplication, compression and file cloning. """

    @property
    def resource(self):
        return VolumeEfficiencySpaceSavings

    gettable_fields = [
        "compression",
        "compression_percent",
        "dedupe",
        "dedupe_percent",
        "dedupe_sharing",
        "total",
        "total_percent",
    ]
    """compression,compression_percent,dedupe,dedupe_percent,dedupe_sharing,total,total_percent,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class VolumeEfficiencySpaceSavings(Resource):

    _schema = VolumeEfficiencySpaceSavingsSchema
