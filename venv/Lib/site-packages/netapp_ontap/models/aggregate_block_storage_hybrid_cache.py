r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AggregateBlockStorageHybridCache", "AggregateBlockStorageHybridCacheSchema"]
__pdoc__ = {
    "AggregateBlockStorageHybridCacheSchema.resource": False,
    "AggregateBlockStorageHybridCacheSchema.opts": False,
    "AggregateBlockStorageHybridCache": False,
}


class AggregateBlockStorageHybridCacheSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AggregateBlockStorageHybridCache object"""

    disk_count = Size(data_key="disk_count")
    r""" Number of disks used in the cache tier of the aggregate. Only provided when hybrid_cache.enabled is 'true'.

Example: 6 """

    disk_type = fields.Str(data_key="disk_type")
    r""" Type of disk being used by the aggregate's cache tier.

Valid choices:

* fc
* lun
* nl_sas
* nvme_ssd
* sas
* sata
* scsi
* ssd
* ssd_cap
* ssd_zns
* vm_disk """

    enabled = fields.Boolean(data_key="enabled")
    r""" Specifies whether the aggregate uses HDDs with SSDs as a cache. """

    raid_size = Size(data_key="raid_size")
    r""" Option to specify the maximum number of disks that can be included in a RAID group.

Example: 24 """

    raid_type = fields.Str(data_key="raid_type")
    r""" RAID type for SSD cache of the aggregate. Only provided when hybrid_cache.enabled is 'true'.

Valid choices:

* raid_dp
* raid_tec
* raid4 """

    simulated_raid_groups = fields.List(fields.Nested("netapp_ontap.models.aggregate_block_storage_hybrid_cache_simulated_raid_groups.AggregateBlockStorageHybridCacheSimulatedRaidGroupsSchema", unknown=EXCLUDE), data_key="simulated_raid_groups")
    r""" The simulated_raid_groups field of the aggregate_block_storage_hybrid_cache. """

    size = Size(data_key="size")
    r""" Total usable space in bytes of SSD cache. Only provided when hybrid_cache.enabled is 'true'.

Example: 1612709888 """

    storage_pools = fields.List(fields.Nested("netapp_ontap.models.aggregate_block_storage_hybrid_cache_storage_pools.AggregateBlockStorageHybridCacheStoragePoolsSchema", unknown=EXCLUDE), data_key="storage_pools")
    r""" List of storage pool properties and allocation_units_count for aggregate. """

    used = Size(data_key="used")
    r""" Space used in bytes of SSD cache. Only provided when hybrid_cache.enabled is 'true'.

Example: 26501122 """

    @property
    def resource(self):
        return AggregateBlockStorageHybridCache

    gettable_fields = [
        "disk_count",
        "disk_type",
        "enabled",
        "raid_size",
        "raid_type",
        "simulated_raid_groups",
        "size",
        "storage_pools",
        "used",
    ]
    """disk_count,disk_type,enabled,raid_size,raid_type,simulated_raid_groups,size,storage_pools,used,"""

    patchable_fields = [
        "disk_count",
        "raid_size",
        "raid_type",
        "simulated_raid_groups",
        "storage_pools",
    ]
    """disk_count,raid_size,raid_type,simulated_raid_groups,storage_pools,"""

    postable_fields = [
        "disk_count",
        "raid_size",
        "raid_type",
        "simulated_raid_groups",
        "storage_pools",
    ]
    """disk_count,raid_size,raid_type,simulated_raid_groups,storage_pools,"""


class AggregateBlockStorageHybridCache(Resource):

    _schema = AggregateBlockStorageHybridCacheSchema
