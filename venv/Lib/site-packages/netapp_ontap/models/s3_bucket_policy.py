r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3BucketPolicy", "S3BucketPolicySchema"]
__pdoc__ = {
    "S3BucketPolicySchema.resource": False,
    "S3BucketPolicySchema.opts": False,
    "S3BucketPolicy": False,
}


class S3BucketPolicySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3BucketPolicy object"""

    statements = fields.List(fields.Nested("netapp_ontap.models.s3_bucket_policy_statement.S3BucketPolicyStatementSchema", unknown=EXCLUDE), data_key="statements")
    r""" Specifies bucket access policy statement. """

    @property
    def resource(self):
        return S3BucketPolicy

    gettable_fields = [
        "statements",
    ]
    """statements,"""

    patchable_fields = [
        "statements",
    ]
    """statements,"""

    postable_fields = [
        "statements",
    ]
    """statements,"""


class S3BucketPolicy(Resource):

    _schema = S3BucketPolicySchema
