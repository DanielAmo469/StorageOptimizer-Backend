r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["GcpKmsKey", "GcpKmsKeySchema"]
__pdoc__ = {
    "GcpKmsKeySchema.resource": False,
    "GcpKmsKeySchema.opts": False,
    "GcpKmsKey": False,
}


class GcpKmsKeySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GcpKmsKey object"""

    key_name = fields.Str(data_key="key_name")
    r""" Key identifier of the Google Cloud KMS key encryption key.

Example: cryptokey1 """

    scope = fields.Str(data_key="scope")
    r""" Set to "svm" for interfaces owned by an SVM. Otherwise, set to "cluster".

Valid choices:

* svm
* cluster """

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", unknown=EXCLUDE, data_key="svm")
    r""" The svm field of the gcp_kms_key. """

    @property
    def resource(self):
        return GcpKmsKey

    gettable_fields = [
        "key_name",
        "scope",
        "svm.links",
        "svm.name",
        "svm.uuid",
    ]
    """key_name,scope,svm.links,svm.name,svm.uuid,"""

    patchable_fields = [
        "svm.name",
        "svm.uuid",
    ]
    """svm.name,svm.uuid,"""

    postable_fields = [
        "key_name",
        "svm.name",
        "svm.uuid",
    ]
    """key_name,svm.name,svm.uuid,"""


class GcpKmsKey(Resource):

    _schema = GcpKmsKeySchema
