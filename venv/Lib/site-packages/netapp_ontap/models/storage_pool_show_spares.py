r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StoragePoolShowSpares", "StoragePoolShowSparesSchema"]
__pdoc__ = {
    "StoragePoolShowSparesSchema.resource": False,
    "StoragePoolShowSparesSchema.opts": False,
    "StoragePoolShowSpares": False,
}


class StoragePoolShowSparesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StoragePoolShowSpares object"""

    checksum_style = fields.Str(data_key="checksum_style")
    r""" The checksum type that has been assigned to the spares.

Valid choices:

* block
* advanced_zoned """

    disk_class = fields.Str(data_key="disk_class")
    r""" Disk class of spares.

Valid choices:

* unknown
* capacity
* performance
* archive
* solid_state
* array
* virtual
* data_center
* capacity_flash """

    disk_type = fields.Str(data_key="disk_type")
    r""" Type of disk.

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

    nodes = fields.List(fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE), data_key="nodes")
    r""" Nodes that can use the usable spares for storage pool. """

    size = Size(data_key="size")
    r""" Usable size of each spare, in bytes.

Example: 10156769280 """

    syncmirror_pool = fields.Str(data_key="syncmirror_pool")
    r""" SyncMirror spare pool.

Valid choices:

* pool0
* pool1 """

    usable = Size(data_key="usable")
    r""" Total number of usable spares in the bucket. The usable count for each class of spares does not include reserved spare capacity recommended by ONTAP best practices.

Example: 9 """

    @property
    def resource(self):
        return StoragePoolShowSpares

    gettable_fields = [
        "checksum_style",
        "disk_class",
        "disk_type",
        "nodes",
        "size",
        "syncmirror_pool",
        "usable",
    ]
    """checksum_style,disk_class,disk_type,nodes,size,syncmirror_pool,usable,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class StoragePoolShowSpares(Resource):

    _schema = StoragePoolShowSparesSchema
