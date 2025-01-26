r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CloudStorageTier", "CloudStorageTierSchema"]
__pdoc__ = {
    "CloudStorageTierSchema.resource": False,
    "CloudStorageTierSchema.opts": False,
    "CloudStorageTier": False,
}


class CloudStorageTierSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CloudStorageTier object"""

    cloud_store = fields.Nested("netapp_ontap.resources.cloud_store.CloudStoreSchema", unknown=EXCLUDE, data_key="cloud_store")
    r""" The cloud_store field of the cloud_storage_tier. """

    used = Size(data_key="used")
    r""" Capacity used in bytes in the cloud store by this aggregate. This is a cached value calculated every 5 minutes. """

    @property
    def resource(self):
        return CloudStorageTier

    gettable_fields = [
        "cloud_store.links",
        "cloud_store.name",
        "cloud_store.uuid",
        "used",
    ]
    """cloud_store.links,cloud_store.name,cloud_store.uuid,used,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class CloudStorageTier(Resource):

    _schema = CloudStorageTierSchema
