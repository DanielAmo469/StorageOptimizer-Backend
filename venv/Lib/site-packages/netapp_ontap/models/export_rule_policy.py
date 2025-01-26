r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ExportRulePolicy", "ExportRulePolicySchema"]
__pdoc__ = {
    "ExportRulePolicySchema.resource": False,
    "ExportRulePolicySchema.opts": False,
    "ExportRulePolicy": False,
}


class ExportRulePolicySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ExportRulePolicy object"""

    id = Size(data_key="id")
    r""" Export policy ID """

    name = fields.Str(data_key="name")
    r""" Export policy name """

    @property
    def resource(self):
        return ExportRulePolicy

    gettable_fields = [
        "id",
        "name",
    ]
    """id,name,"""

    patchable_fields = [
        "id",
        "name",
    ]
    """id,name,"""

    postable_fields = [
        "id",
        "name",
    ]
    """id,name,"""


class ExportRulePolicy(Resource):

    _schema = ExportRulePolicySchema
