r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SoftwareValidationReferenceIssue", "SoftwareValidationReferenceIssueSchema"]
__pdoc__ = {
    "SoftwareValidationReferenceIssueSchema.resource": False,
    "SoftwareValidationReferenceIssueSchema.opts": False,
    "SoftwareValidationReferenceIssue": False,
}


class SoftwareValidationReferenceIssueSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SoftwareValidationReferenceIssue object"""

    message = fields.Str(data_key="message")
    r""" Details of the error or warning encountered by the update checks.

Example: Cluster HA is not configured in the cluster. """

    @property
    def resource(self):
        return SoftwareValidationReferenceIssue

    gettable_fields = [
        "message",
    ]
    """message,"""

    patchable_fields = [
        "message",
    ]
    """message,"""

    postable_fields = [
        "message",
    ]
    """message,"""


class SoftwareValidationReferenceIssue(Resource):

    _schema = SoftwareValidationReferenceIssueSchema
