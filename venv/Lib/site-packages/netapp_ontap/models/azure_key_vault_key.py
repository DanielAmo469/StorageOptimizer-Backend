r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AzureKeyVaultKey", "AzureKeyVaultKeySchema"]
__pdoc__ = {
    "AzureKeyVaultKeySchema.resource": False,
    "AzureKeyVaultKeySchema.opts": False,
    "AzureKeyVaultKey": False,
}


class AzureKeyVaultKeySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AzureKeyVaultKey object"""

    key_id = fields.Str(data_key="key_id")
    r""" Key identifier of the AKV key encryption key.

Example: https://keyvault1.vault.azure.net/keys/key1/12345678901234567890123456789012 """

    scope = fields.Str(data_key="scope")
    r""" Set to "svm" for interfaces owned by an SVM. Otherwise, set to "cluster".

Valid choices:

* svm
* cluster """

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", unknown=EXCLUDE, data_key="svm")
    r""" The svm field of the azure_key_vault_key. """

    @property
    def resource(self):
        return AzureKeyVaultKey

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


class AzureKeyVaultKey(Resource):

    _schema = AzureKeyVaultKeySchema
