r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AutosupportConnectivityIssue", "AutosupportConnectivityIssueSchema"]
__pdoc__ = {
    "AutosupportConnectivityIssueSchema.resource": False,
    "AutosupportConnectivityIssueSchema.opts": False,
    "AutosupportConnectivityIssue": False,
}


class AutosupportConnectivityIssueSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AutosupportConnectivityIssue object"""

    code = fields.Str(data_key="code")
    r""" Error code

Example: 53149746 """

    message = fields.Str(data_key="message")
    r""" Error message

Example: SMTP connectivity check failed for destination: mailhost. Error: Could not resolve host - 'mailhost' """

    @property
    def resource(self):
        return AutosupportConnectivityIssue

    gettable_fields = [
        "code",
        "message",
    ]
    """code,message,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class AutosupportConnectivityIssue(Resource):

    _schema = AutosupportConnectivityIssueSchema
