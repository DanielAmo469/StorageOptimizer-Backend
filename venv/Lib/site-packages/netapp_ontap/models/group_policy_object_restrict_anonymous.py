r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["GroupPolicyObjectRestrictAnonymous", "GroupPolicyObjectRestrictAnonymousSchema"]
__pdoc__ = {
    "GroupPolicyObjectRestrictAnonymousSchema.resource": False,
    "GroupPolicyObjectRestrictAnonymousSchema.opts": False,
    "GroupPolicyObjectRestrictAnonymous": False,
}


class GroupPolicyObjectRestrictAnonymousSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GroupPolicyObjectRestrictAnonymous object"""

    anonymous_access_to_shares_and_named_pipes_restricted = fields.Boolean(data_key="anonymous_access_to_shares_and_named_pipes_restricted")
    r""" Restrict anonymous access to shares and named pipes. """

    combined_restriction_for_anonymous_user = fields.Str(data_key="combined_restriction_for_anonymous_user")
    r""" Combined restriction for anonymous user.

Valid choices:

* no_restriction
* no_enumeration
* no_access """

    no_enumeration_of_sam_accounts = fields.Boolean(data_key="no_enumeration_of_sam_accounts")
    r""" No enumeration of SAM accounts. """

    no_enumeration_of_sam_accounts_and_shares = fields.Boolean(data_key="no_enumeration_of_sam_accounts_and_shares")
    r""" No enumeration of SAM accounts and shares. """

    @property
    def resource(self):
        return GroupPolicyObjectRestrictAnonymous

    gettable_fields = [
        "anonymous_access_to_shares_and_named_pipes_restricted",
        "combined_restriction_for_anonymous_user",
        "no_enumeration_of_sam_accounts",
        "no_enumeration_of_sam_accounts_and_shares",
    ]
    """anonymous_access_to_shares_and_named_pipes_restricted,combined_restriction_for_anonymous_user,no_enumeration_of_sam_accounts,no_enumeration_of_sam_accounts_and_shares,"""

    patchable_fields = [
        "anonymous_access_to_shares_and_named_pipes_restricted",
        "combined_restriction_for_anonymous_user",
        "no_enumeration_of_sam_accounts",
        "no_enumeration_of_sam_accounts_and_shares",
    ]
    """anonymous_access_to_shares_and_named_pipes_restricted,combined_restriction_for_anonymous_user,no_enumeration_of_sam_accounts,no_enumeration_of_sam_accounts_and_shares,"""

    postable_fields = [
        "anonymous_access_to_shares_and_named_pipes_restricted",
        "combined_restriction_for_anonymous_user",
        "no_enumeration_of_sam_accounts",
        "no_enumeration_of_sam_accounts_and_shares",
    ]
    """anonymous_access_to_shares_and_named_pipes_restricted,combined_restriction_for_anonymous_user,no_enumeration_of_sam_accounts,no_enumeration_of_sam_accounts_and_shares,"""


class GroupPolicyObjectRestrictAnonymous(Resource):

    _schema = GroupPolicyObjectRestrictAnonymousSchema
