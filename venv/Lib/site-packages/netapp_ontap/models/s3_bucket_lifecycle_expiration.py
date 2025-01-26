r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3BucketLifecycleExpiration", "S3BucketLifecycleExpirationSchema"]
__pdoc__ = {
    "S3BucketLifecycleExpirationSchema.resource": False,
    "S3BucketLifecycleExpirationSchema.opts": False,
    "S3BucketLifecycleExpiration": False,
}


class S3BucketLifecycleExpirationSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3BucketLifecycleExpiration object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the s3_bucket_lifecycle_expiration. """

    expired_object_delete_marker = fields.Boolean(data_key="expired_object_delete_marker")
    r""" Cleanup object delete markers. """

    object_age_days = Size(data_key="object_age_days")
    r""" Number of days since creation after which objects can be deleted. """

    object_expiry_date = ImpreciseDateTime(data_key="object_expiry_date")
    r""" Specific date from when objects can expire. """

    @property
    def resource(self):
        return S3BucketLifecycleExpiration

    gettable_fields = [
        "links",
        "expired_object_delete_marker",
        "object_age_days",
        "object_expiry_date",
    ]
    """links,expired_object_delete_marker,object_age_days,object_expiry_date,"""

    patchable_fields = [
        "expired_object_delete_marker",
        "object_age_days",
        "object_expiry_date",
    ]
    """expired_object_delete_marker,object_age_days,object_expiry_date,"""

    postable_fields = [
        "expired_object_delete_marker",
        "object_age_days",
        "object_expiry_date",
    ]
    """expired_object_delete_marker,object_age_days,object_expiry_date,"""


class S3BucketLifecycleExpiration(Resource):

    _schema = S3BucketLifecycleExpirationSchema
