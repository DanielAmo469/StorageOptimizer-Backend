r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3BucketSvmLifecycleManagement", "S3BucketSvmLifecycleManagementSchema"]
__pdoc__ = {
    "S3BucketSvmLifecycleManagementSchema.resource": False,
    "S3BucketSvmLifecycleManagementSchema.opts": False,
    "S3BucketSvmLifecycleManagement": False,
}


class S3BucketSvmLifecycleManagementSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3BucketSvmLifecycleManagement object"""

    rules = fields.List(fields.Nested("netapp_ontap.models.s3_bucket_lifecycle_management_rules.S3BucketLifecycleManagementRulesSchema", unknown=EXCLUDE), data_key="rules")
    r""" Specifies an object store lifecycle management policy. """

    @property
    def resource(self):
        return S3BucketSvmLifecycleManagement

    gettable_fields = [
        "rules",
    ]
    """rules,"""

    patchable_fields = [
        "rules",
    ]
    """rules,"""

    postable_fields = [
        "rules",
    ]
    """rules,"""


class S3BucketSvmLifecycleManagement(Resource):

    _schema = S3BucketSvmLifecycleManagementSchema
