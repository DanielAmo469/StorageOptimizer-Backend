r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["GroupPolicyObjectRegistryValue", "GroupPolicyObjectRegistryValueSchema"]
__pdoc__ = {
    "GroupPolicyObjectRegistryValueSchema.resource": False,
    "GroupPolicyObjectRegistryValueSchema.opts": False,
    "GroupPolicyObjectRegistryValue": False,
}


class GroupPolicyObjectRegistryValueSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GroupPolicyObjectRegistryValue object"""

    signing_required = fields.Boolean(data_key="signing_required")
    r""" SMB signing required. """

    @property
    def resource(self):
        return GroupPolicyObjectRegistryValue

    gettable_fields = [
        "signing_required",
    ]
    """signing_required,"""

    patchable_fields = [
        "signing_required",
    ]
    """signing_required,"""

    postable_fields = [
        "signing_required",
    ]
    """signing_required,"""


class GroupPolicyObjectRegistryValue(Resource):

    _schema = GroupPolicyObjectRegistryValueSchema
