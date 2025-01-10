r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["GroupPolicyObjectEventLog", "GroupPolicyObjectEventLogSchema"]
__pdoc__ = {
    "GroupPolicyObjectEventLogSchema.resource": False,
    "GroupPolicyObjectEventLogSchema.opts": False,
    "GroupPolicyObjectEventLog": False,
}


class GroupPolicyObjectEventLogSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GroupPolicyObjectEventLog object"""

    max_size = Size(data_key="max_size")
    r""" Maximum size of security log, in kilobytes.

Example: 2048 """

    retention_method = fields.Str(data_key="retention_method")
    r""" Audit log retention method.

Valid choices:

* overwrite_as_needed
* overwrite_by_days
* do_not_overwrite """

    @property
    def resource(self):
        return GroupPolicyObjectEventLog

    gettable_fields = [
        "max_size",
        "retention_method",
    ]
    """max_size,retention_method,"""

    patchable_fields = [
        "max_size",
        "retention_method",
    ]
    """max_size,retention_method,"""

    postable_fields = [
        "max_size",
        "retention_method",
    ]
    """max_size,retention_method,"""


class GroupPolicyObjectEventLog(Resource):

    _schema = GroupPolicyObjectEventLogSchema
