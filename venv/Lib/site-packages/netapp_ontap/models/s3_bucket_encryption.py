r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3BucketEncryption", "S3BucketEncryptionSchema"]
__pdoc__ = {
    "S3BucketEncryptionSchema.resource": False,
    "S3BucketEncryptionSchema.opts": False,
    "S3BucketEncryption": False,
}


class S3BucketEncryptionSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3BucketEncryption object"""

    enabled = fields.Boolean(data_key="enabled")
    r""" Specifies whether encryption is enabled on the bucket. By default, encryption is disabled on a bucket. """

    @property
    def resource(self):
        return S3BucketEncryption

    gettable_fields = [
        "enabled",
    ]
    """enabled,"""

    patchable_fields = [
        "enabled",
    ]
    """enabled,"""

    postable_fields = [
        "enabled",
    ]
    """enabled,"""


class S3BucketEncryption(Resource):

    _schema = S3BucketEncryptionSchema
