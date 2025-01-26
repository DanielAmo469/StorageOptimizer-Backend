r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CloudStoreAggregate", "CloudStoreAggregateSchema"]
__pdoc__ = {
    "CloudStoreAggregateSchema.resource": False,
    "CloudStoreAggregateSchema.opts": False,
    "CloudStoreAggregate": False,
}


class CloudStoreAggregateSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CloudStoreAggregate object"""

    name = fields.Str(data_key="name")
    r""" The name field of the cloud_store_aggregate.

Example: aggr1 """

    @property
    def resource(self):
        return CloudStoreAggregate

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class CloudStoreAggregate(Resource):

    _schema = CloudStoreAggregateSchema
