r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3BucketLifecycleAbortIncompleteMultipartUpload", "S3BucketLifecycleAbortIncompleteMultipartUploadSchema"]
__pdoc__ = {
    "S3BucketLifecycleAbortIncompleteMultipartUploadSchema.resource": False,
    "S3BucketLifecycleAbortIncompleteMultipartUploadSchema.opts": False,
    "S3BucketLifecycleAbortIncompleteMultipartUpload": False,
}


class S3BucketLifecycleAbortIncompleteMultipartUploadSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3BucketLifecycleAbortIncompleteMultipartUpload object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the s3_bucket_lifecycle_abort_incomplete_multipart_upload. """

    after_initiation_days = Size(data_key="after_initiation_days")
    r""" Number of days of initiation after which uploads can be aborted. """

    @property
    def resource(self):
        return S3BucketLifecycleAbortIncompleteMultipartUpload

    gettable_fields = [
        "links",
        "after_initiation_days",
    ]
    """links,after_initiation_days,"""

    patchable_fields = [
        "after_initiation_days",
    ]
    """after_initiation_days,"""

    postable_fields = [
        "after_initiation_days",
    ]
    """after_initiation_days,"""


class S3BucketLifecycleAbortIncompleteMultipartUpload(Resource):

    _schema = S3BucketLifecycleAbortIncompleteMultipartUploadSchema
