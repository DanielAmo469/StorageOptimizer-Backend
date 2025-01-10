r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["GroupPolicyObjectKerberos", "GroupPolicyObjectKerberosSchema"]
__pdoc__ = {
    "GroupPolicyObjectKerberosSchema.resource": False,
    "GroupPolicyObjectKerberosSchema.opts": False,
    "GroupPolicyObjectKerberos": False,
}


class GroupPolicyObjectKerberosSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GroupPolicyObjectKerberos object"""

    max_clock_skew = fields.Str(data_key="max_clock_skew")
    r""" Kerberos clock skew in ISO-8601 format.

Example: P15M """

    max_renew_age = fields.Str(data_key="max_renew_age")
    r""" Kerberos max renew age in ISO-8601 format.

Example: P2D """

    max_ticket_age = fields.Str(data_key="max_ticket_age")
    r""" Kerberos max ticket age in ISO-8601 format.

Example: P24H """

    @property
    def resource(self):
        return GroupPolicyObjectKerberos

    gettable_fields = [
        "max_clock_skew",
        "max_renew_age",
        "max_ticket_age",
    ]
    """max_clock_skew,max_renew_age,max_ticket_age,"""

    patchable_fields = [
        "max_clock_skew",
        "max_renew_age",
        "max_ticket_age",
    ]
    """max_clock_skew,max_renew_age,max_ticket_age,"""

    postable_fields = [
        "max_clock_skew",
        "max_renew_age",
        "max_ticket_age",
    ]
    """max_clock_skew,max_renew_age,max_ticket_age,"""


class GroupPolicyObjectKerberos(Resource):

    _schema = GroupPolicyObjectKerberosSchema
