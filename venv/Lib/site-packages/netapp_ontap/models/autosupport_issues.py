r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AutosupportIssues", "AutosupportIssuesSchema"]
__pdoc__ = {
    "AutosupportIssuesSchema.resource": False,
    "AutosupportIssuesSchema.opts": False,
    "AutosupportIssues": False,
}


class AutosupportIssuesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AutosupportIssues object"""

    corrective_action = fields.Nested("netapp_ontap.models.autosupport_connectivity_corrective_action.AutosupportConnectivityCorrectiveActionSchema", unknown=EXCLUDE, data_key="corrective_action")
    r""" The corrective_action field of the autosupport_issues. """

    issue = fields.Nested("netapp_ontap.models.autosupport_connectivity_issue.AutosupportConnectivityIssueSchema", unknown=EXCLUDE, data_key="issue")
    r""" The issue field of the autosupport_issues. """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the autosupport_issues. """

    @property
    def resource(self):
        return AutosupportIssues

    gettable_fields = [
        "corrective_action",
        "issue",
        "node.links",
        "node.name",
        "node.uuid",
    ]
    """corrective_action,issue,node.links,node.name,node.uuid,"""

    patchable_fields = [
        "corrective_action",
        "issue",
        "node.name",
        "node.uuid",
    ]
    """corrective_action,issue,node.name,node.uuid,"""

    postable_fields = [
        "corrective_action",
        "issue",
        "node.name",
        "node.uuid",
    ]
    """corrective_action,issue,node.name,node.uuid,"""


class AutosupportIssues(Resource):

    _schema = AutosupportIssuesSchema
