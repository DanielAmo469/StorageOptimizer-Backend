r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AggregateBlockStorageHybridCacheStoragePools", "AggregateBlockStorageHybridCacheStoragePoolsSchema"]
__pdoc__ = {
    "AggregateBlockStorageHybridCacheStoragePoolsSchema.resource": False,
    "AggregateBlockStorageHybridCacheStoragePoolsSchema.opts": False,
    "AggregateBlockStorageHybridCacheStoragePools": False,
}


class AggregateBlockStorageHybridCacheStoragePoolsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AggregateBlockStorageHybridCacheStoragePools object"""

    allocation_units_count = Size(data_key="allocation_units_count")
    r""" Allocation count of storage pool. """

    storage_pool = fields.Nested("netapp_ontap.resources.storage_pool.StoragePoolSchema", unknown=EXCLUDE, data_key="storage_pool")
    r""" The storage_pool field of the aggregate_block_storage_hybrid_cache_storage_pools. """

    @property
    def resource(self):
        return AggregateBlockStorageHybridCacheStoragePools

    gettable_fields = [
        "allocation_units_count",
        "storage_pool.links",
        "storage_pool.name",
        "storage_pool.uuid",
    ]
    """allocation_units_count,storage_pool.links,storage_pool.name,storage_pool.uuid,"""

    patchable_fields = [
        "allocation_units_count",
        "storage_pool.name",
        "storage_pool.uuid",
    ]
    """allocation_units_count,storage_pool.name,storage_pool.uuid,"""

    postable_fields = [
        "allocation_units_count",
        "storage_pool.name",
        "storage_pool.uuid",
    ]
    """allocation_units_count,storage_pool.name,storage_pool.uuid,"""


class AggregateBlockStorageHybridCacheStoragePools(Resource):

    _schema = AggregateBlockStorageHybridCacheStoragePoolsSchema
