r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["GroupPolicyObjectEventAudit", "GroupPolicyObjectEventAuditSchema"]
__pdoc__ = {
    "GroupPolicyObjectEventAuditSchema.resource": False,
    "GroupPolicyObjectEventAuditSchema.opts": False,
    "GroupPolicyObjectEventAudit": False,
}


class GroupPolicyObjectEventAuditSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GroupPolicyObjectEventAudit object"""

    logon_type = fields.Str(data_key="logon_type")
    r""" Type of logon event to be audited.

Valid choices:

* none
* success
* failure
* both """

    object_access_type = fields.Str(data_key="object_access_type")
    r""" Type of object access to be audited.

Valid choices:

* none
* success
* failure
* both """

    @property
    def resource(self):
        return GroupPolicyObjectEventAudit

    gettable_fields = [
        "logon_type",
        "object_access_type",
    ]
    """logon_type,object_access_type,"""

    patchable_fields = [
        "logon_type",
        "object_access_type",
    ]
    """logon_type,object_access_type,"""

    postable_fields = [
        "logon_type",
        "object_access_type",
    ]
    """logon_type,object_access_type,"""


class GroupPolicyObjectEventAudit(Resource):

    _schema = GroupPolicyObjectEventAuditSchema
