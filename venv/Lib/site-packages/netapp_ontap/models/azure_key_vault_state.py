r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AzureKeyVaultState", "AzureKeyVaultStateSchema"]
__pdoc__ = {
    "AzureKeyVaultStateSchema.resource": False,
    "AzureKeyVaultStateSchema.opts": False,
    "AzureKeyVaultState": False,
}


class AzureKeyVaultStateSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AzureKeyVaultState object"""

    available = fields.Boolean(data_key="available")
    r""" Set to true when an AKV wrapped internal key is present on all nodes of the cluster. """

    code = fields.Str(data_key="code")
    r""" Code corresponding to the status message. Returns a 0 if AKV wrapped key is available on all nodes in the cluster.

Example: 346758 """

    message = fields.Str(data_key="message")
    r""" Error message set when top-level internal key protection key (KEK) availability on cluster is false.

Example: Top-level internal key protection key (KEK) is unavailable on the following nodes with the associated reasons: Node: node1. Reason: No volumes created yet for the SVM. Wrapped KEK status will be available after creating encrypted volumes. """

    @property
    def resource(self):
        return AzureKeyVaultState

    gettable_fields = [
        "available",
        "code",
        "message",
    ]
    """available,code,message,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class AzureKeyVaultState(Resource):

    _schema = AzureKeyVaultStateSchema
