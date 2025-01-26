r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SoftwareStatusDetailsReferenceIssue", "SoftwareStatusDetailsReferenceIssueSchema"]
__pdoc__ = {
    "SoftwareStatusDetailsReferenceIssueSchema.resource": False,
    "SoftwareStatusDetailsReferenceIssueSchema.opts": False,
    "SoftwareStatusDetailsReferenceIssue": False,
}


class SoftwareStatusDetailsReferenceIssueSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SoftwareStatusDetailsReferenceIssue object"""

    code = Size(data_key="code")
    r""" Error code corresponding to update status

Example: 10551399 """

    message = fields.Str(data_key="message")
    r""" Update status details

Example: Image update complete """

    @property
    def resource(self):
        return SoftwareStatusDetailsReferenceIssue

    gettable_fields = [
        "code",
        "message",
    ]
    """code,message,"""

    patchable_fields = [
        "code",
        "message",
    ]
    """code,message,"""

    postable_fields = [
        "code",
        "message",
    ]
    """code,message,"""


class SoftwareStatusDetailsReferenceIssue(Resource):

    _schema = SoftwareStatusDetailsReferenceIssueSchema
