r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeSubsystemHostRecordsIoQueue", "NvmeSubsystemHostRecordsIoQueueSchema"]
__pdoc__ = {
    "NvmeSubsystemHostRecordsIoQueueSchema.resource": False,
    "NvmeSubsystemHostRecordsIoQueueSchema.opts": False,
    "NvmeSubsystemHostRecordsIoQueue": False,
}


class NvmeSubsystemHostRecordsIoQueueSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeSubsystemHostRecordsIoQueue object"""

    count = Size(data_key="count")
    r""" The number of I/O queue pairs. The default value is inherited from the owning NVMe subsystem.


Example: 4 """

    depth = Size(data_key="depth")
    r""" The I/O queue depth. The default value is inherited from the owning NVMe subsystem.


Example: 32 """

    @property
    def resource(self):
        return NvmeSubsystemHostRecordsIoQueue

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


class NvmeSubsystemHostRecordsIoQueue(Resource):

    _schema = NvmeSubsystemHostRecordsIoQueueSchema
