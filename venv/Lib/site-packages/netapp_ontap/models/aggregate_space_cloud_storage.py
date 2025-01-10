r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AggregateSpaceCloudStorage", "AggregateSpaceCloudStorageSchema"]
__pdoc__ = {
    "AggregateSpaceCloudStorageSchema.resource": False,
    "AggregateSpaceCloudStorageSchema.opts": False,
    "AggregateSpaceCloudStorage": False,
}


class AggregateSpaceCloudStorageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AggregateSpaceCloudStorage object"""

    used = Size(data_key="used")
    r""" Used space in bytes in the cloud store. Only applicable for aggregates with a cloud store tier.

Example: 402743264 """

    @property
    def resource(self):
        return AggregateSpaceCloudStorage

    gettable_fields = [
        "used",
    ]
    """used,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class AggregateSpaceCloudStorage(Resource):

    _schema = AggregateSpaceCloudStorageSchema
