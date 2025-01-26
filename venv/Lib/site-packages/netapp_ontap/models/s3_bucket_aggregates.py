r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3BucketAggregates", "S3BucketAggregatesSchema"]
__pdoc__ = {
    "S3BucketAggregatesSchema.resource": False,
    "S3BucketAggregatesSchema.opts": False,
    "S3BucketAggregates": False,
}


class S3BucketAggregatesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3BucketAggregates object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the s3_bucket_aggregates. """

    name = fields.Str(data_key="name")
    r""" The name field of the s3_bucket_aggregates.

Example: aggr1 """

    uuid = fields.Str(data_key="uuid")
    r""" The uuid field of the s3_bucket_aggregates.

Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return S3BucketAggregates

    gettable_fields = [
        "links",
        "name",
        "uuid",
    ]
    """links,name,uuid,"""

    patchable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class S3BucketAggregates(Resource):

    _schema = S3BucketAggregatesSchema
