r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ZappS3Bucket", "ZappS3BucketSchema"]
__pdoc__ = {
    "ZappS3BucketSchema.resource": False,
    "ZappS3BucketSchema.opts": False,
    "ZappS3Bucket": False,
}


class ZappS3BucketSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ZappS3Bucket object"""

    application_components = fields.List(fields.Nested("netapp_ontap.models.zapp_s3_bucket_application_components.ZappS3BucketApplicationComponentsSchema", unknown=EXCLUDE), data_key="application_components")
    r""" The list of application components to be created. """

    @property
    def resource(self):
        return ZappS3Bucket

    gettable_fields = [
        "application_components",
    ]
    """application_components,"""

    patchable_fields = [
        "application_components",
    ]
    """application_components,"""

    postable_fields = [
        "application_components",
    ]
    """application_components,"""


class ZappS3Bucket(Resource):

    _schema = ZappS3BucketSchema
