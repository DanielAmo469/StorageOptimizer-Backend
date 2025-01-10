r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3BucketSvmLifecycleManagementRules", "S3BucketSvmLifecycleManagementRulesSchema"]
__pdoc__ = {
    "S3BucketSvmLifecycleManagementRulesSchema.resource": False,
    "S3BucketSvmLifecycleManagementRulesSchema.opts": False,
    "S3BucketSvmLifecycleManagementRules": False,
}


class S3BucketSvmLifecycleManagementRulesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3BucketSvmLifecycleManagementRules object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the s3_bucket_svm_lifecycle_management_rules. """

    abort_incomplete_multipart_upload = fields.Nested("netapp_ontap.models.s3_bucket_lifecycle_abort_incomplete_multipart_upload.S3BucketLifecycleAbortIncompleteMultipartUploadSchema", unknown=EXCLUDE, data_key="abort_incomplete_multipart_upload")
    r""" The abort_incomplete_multipart_upload field of the s3_bucket_svm_lifecycle_management_rules. """

    enabled = fields.Boolean(data_key="enabled")
    r""" Specifies whether or not the associated rule is enabled. """

    expiration = fields.Nested("netapp_ontap.models.s3_bucket_lifecycle_expiration.S3BucketLifecycleExpirationSchema", unknown=EXCLUDE, data_key="expiration")
    r""" The expiration field of the s3_bucket_svm_lifecycle_management_rules. """

    name = fields.Str(data_key="name")
    r""" Bucket lifecycle management rule identifier. """

    non_current_version_expiration = fields.Nested("netapp_ontap.models.s3_bucket_lifecycle_non_current_version_expiration.S3BucketLifecycleNonCurrentVersionExpirationSchema", unknown=EXCLUDE, data_key="non_current_version_expiration")
    r""" The non_current_version_expiration field of the s3_bucket_svm_lifecycle_management_rules. """

    object_filter = fields.Nested("netapp_ontap.models.s3_bucket_lifecycle_object_filter.S3BucketLifecycleObjectFilterSchema", unknown=EXCLUDE, data_key="object_filter")
    r""" The object_filter field of the s3_bucket_svm_lifecycle_management_rules. """

    @property
    def resource(self):
        return S3BucketSvmLifecycleManagementRules

    gettable_fields = [
        "links",
        "abort_incomplete_multipart_upload",
        "enabled",
        "expiration",
        "name",
        "non_current_version_expiration",
        "object_filter",
    ]
    """links,abort_incomplete_multipart_upload,enabled,expiration,name,non_current_version_expiration,object_filter,"""

    patchable_fields = [
        "abort_incomplete_multipart_upload",
        "enabled",
        "expiration",
        "non_current_version_expiration",
    ]
    """abort_incomplete_multipart_upload,enabled,expiration,non_current_version_expiration,"""

    postable_fields = [
        "abort_incomplete_multipart_upload",
        "enabled",
        "expiration",
        "name",
        "non_current_version_expiration",
        "object_filter",
    ]
    """abort_incomplete_multipart_upload,enabled,expiration,name,non_current_version_expiration,object_filter,"""


class S3BucketSvmLifecycleManagementRules(Resource):

    _schema = S3BucketSvmLifecycleManagementRulesSchema
