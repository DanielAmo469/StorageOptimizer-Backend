r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeSubsystemIoQueueDefault", "NvmeSubsystemIoQueueDefaultSchema"]
__pdoc__ = {
    "NvmeSubsystemIoQueueDefaultSchema.resource": False,
    "NvmeSubsystemIoQueueDefaultSchema.opts": False,
    "NvmeSubsystemIoQueueDefault": False,
}


class NvmeSubsystemIoQueueDefaultSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeSubsystemIoQueueDefault object"""

    count = Size(data_key="count")
    r""" The number of host I/O queue pairs.


Example: 4 """

    depth = Size(data_key="depth")
    r""" The host I/O queue depth.


Example: 16 """

    @property
    def resource(self):
        return NvmeSubsystemIoQueueDefault

    gettable_fields = [
        "count",
        "depth",
    ]
    """count,depth,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class NvmeSubsystemIoQueueDefault(Resource):

    _schema = NvmeSubsystemIoQueueDefaultSchema
