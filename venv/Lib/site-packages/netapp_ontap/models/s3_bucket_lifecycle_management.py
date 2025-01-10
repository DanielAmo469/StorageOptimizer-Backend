r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3BucketLifecycleManagement", "S3BucketLifecycleManagementSchema"]
__pdoc__ = {
    "S3BucketLifecycleManagementSchema.resource": False,
    "S3BucketLifecycleManagementSchema.opts": False,
    "S3BucketLifecycleManagement": False,
}


class S3BucketLifecycleManagementSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3BucketLifecycleManagement object"""

    rules = fields.List(fields.Nested("netapp_ontap.resources.s3_bucket_lifecycle_rule.S3BucketLifecycleRuleSchema", unknown=EXCLUDE), data_key="rules")
    r""" Specifies an object store lifecycle management policy. """

    @property
    def resource(self):
        return S3BucketLifecycleManagement

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


class S3BucketLifecycleManagement(Resource):

    _schema = S3BucketLifecycleManagementSchema
