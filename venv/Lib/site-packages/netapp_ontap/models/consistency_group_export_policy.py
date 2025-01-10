r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupExportPolicy", "ConsistencyGroupExportPolicySchema"]
__pdoc__ = {
    "ConsistencyGroupExportPolicySchema.resource": False,
    "ConsistencyGroupExportPolicySchema.opts": False,
    "ConsistencyGroupExportPolicy": False,
}


class ConsistencyGroupExportPolicySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupExportPolicy object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the consistency_group_export_policy. """

    name = fields.Str(data_key="name")
    r""" Name of the export policy. """

    rules = fields.List(fields.Nested("netapp_ontap.models.export_rules.ExportRulesSchema", unknown=EXCLUDE), data_key="rules")
    r""" The set of rules that govern the export policy. """

    uuid = fields.Str(data_key="uuid")
    r""" Identifier for the export policy. """

    @property
    def resource(self):
        return ConsistencyGroupExportPolicy

    gettable_fields = [
        "links",
        "name",
        "rules",
        "uuid",
    ]
    """links,name,rules,uuid,"""

    patchable_fields = [
        "name",
        "rules",
    ]
    """name,rules,"""

    postable_fields = [
        "name",
        "rules",
    ]
    """name,rules,"""


class ConsistencyGroupExportPolicy(Resource):

    _schema = ConsistencyGroupExportPolicySchema
