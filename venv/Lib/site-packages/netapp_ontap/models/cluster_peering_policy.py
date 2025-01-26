r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterPeeringPolicy", "ClusterPeeringPolicySchema"]
__pdoc__ = {
    "ClusterPeeringPolicySchema.resource": False,
    "ClusterPeeringPolicySchema.opts": False,
    "ClusterPeeringPolicy": False,
}


class ClusterPeeringPolicySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterPeeringPolicy object"""

    authentication_required = fields.Boolean(data_key="authentication_required")
    r""" Indicates whether authentication is required in the communication between cluster peers. If true, authentication is required to establish communication between cluster peers. """

    encryption_required = fields.Boolean(data_key="encryption_required")
    r""" Indicates whether encryption is required in the communication between cluster peers. If true, encryption is required to establish communication between cluster peers. """

    minimum_passphrase_length = Size(data_key="minimum_passphrase_length")
    r""" Minimum required length for a passphrase. For more information on password strength best practices, see: https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html#implement-proper-password-strength-controls """

    @property
    def resource(self):
        return ClusterPeeringPolicy

    gettable_fields = [
        "authentication_required",
        "encryption_required",
        "minimum_passphrase_length",
    ]
    """authentication_required,encryption_required,minimum_passphrase_length,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterPeeringPolicy(Resource):

    _schema = ClusterPeeringPolicySchema
