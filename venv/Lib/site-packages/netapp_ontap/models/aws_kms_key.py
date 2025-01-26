r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AwsKmsKey", "AwsKmsKeySchema"]
__pdoc__ = {
    "AwsKmsKeySchema.resource": False,
    "AwsKmsKeySchema.opts": False,
    "AwsKmsKey": False,
}


class AwsKmsKeySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AwsKmsKey object"""

    key_id = fields.Str(data_key="key_id")
    r""" Key identifier of the AWS KMS key encryption key.

Example: key01 """

    scope = fields.Str(data_key="scope")
    r""" Set to "svm" for interfaces owned by an SVM. Otherwise, set to "cluster".

Valid choices:

* svm
* cluster """

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", unknown=EXCLUDE, data_key="svm")
    r""" The svm field of the aws_kms_key. """

    @property
    def resource(self):
        return AwsKmsKey

    gettable_fields = [
        "key_id",
        "scope",
        "svm.links",
        "svm.name",
        "svm.uuid",
    ]
    """key_id,scope,svm.links,svm.name,svm.uuid,"""

    patchable_fields = [
        "svm.name",
        "svm.uuid",
    ]
    """svm.name,svm.uuid,"""

    postable_fields = [
        "key_id",
        "svm.name",
        "svm.uuid",
    ]
    """key_id,svm.name,svm.uuid,"""


class AwsKmsKey(Resource):

    _schema = AwsKmsKeySchema
