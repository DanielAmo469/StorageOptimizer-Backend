r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StoragePoolUsedAllocationUnit", "StoragePoolUsedAllocationUnitSchema"]
__pdoc__ = {
    "StoragePoolUsedAllocationUnitSchema.resource": False,
    "StoragePoolUsedAllocationUnitSchema.opts": False,
    "StoragePoolUsedAllocationUnit": False,
}


class StoragePoolUsedAllocationUnitSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StoragePoolUsedAllocationUnit object"""

    aggregate = fields.Nested("netapp_ontap.resources.aggregate.AggregateSchema", unknown=EXCLUDE, data_key="aggregate")
    r""" The aggregate field of the storage_pool_used_allocation_unit. """

    count = Size(data_key="count")
    r""" The number of allocation units used by this aggregate. """

    current_usage = Size(data_key="current_usage")
    r""" The amount of cache space used by this aggregate. """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the storage_pool_used_allocation_unit. """

    @property
    def resource(self):
        return StoragePoolUsedAllocationUnit

    gettable_fields = [
        "aggregate.links",
        "aggregate.name",
        "aggregate.uuid",
        "count",
        "current_usage",
        "node.links",
        "node.name",
        "node.uuid",
    ]
    """aggregate.links,aggregate.name,aggregate.uuid,count,current_usage,node.links,node.name,node.uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class StoragePoolUsedAllocationUnit(Resource):

    _schema = StoragePoolUsedAllocationUnitSchema
