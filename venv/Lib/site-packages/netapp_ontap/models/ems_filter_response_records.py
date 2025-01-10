r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsFilterResponseRecords", "EmsFilterResponseRecordsSchema"]
__pdoc__ = {
    "EmsFilterResponseRecordsSchema.resource": False,
    "EmsFilterResponseRecordsSchema.opts": False,
    "EmsFilterResponseRecords": False,
}


class EmsFilterResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsFilterResponseRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the ems_filter_response_records. """

    access_control_role = fields.Nested("netapp_ontap.resources.role.RoleSchema", unknown=EXCLUDE, data_key="access_control_role")
    r""" The access_control_role field of the ems_filter_response_records. """

    name = fields.Str(data_key="name")
    r""" Filter name

Example: snmp-traphost """

    rules = fields.List(fields.Nested("netapp_ontap.resources.ems_filter_rule.EmsFilterRuleSchema", unknown=EXCLUDE), data_key="rules")
    r""" Array of event filter rules on which to match. """

    system_defined = fields.Boolean(data_key="system_defined")
    r""" Flag indicating system-defined filters.

Example: true """

    @property
    def resource(self):
        return EmsFilterResponseRecords

    gettable_fields = [
        "links",
        "access_control_role.links",
        "access_control_role.name",
        "name",
        "rules",
        "system_defined",
    ]
    """links,access_control_role.links,access_control_role.name,name,rules,system_defined,"""

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


class EmsFilterResponseRecords(Resource):

    _schema = EmsFilterResponseRecordsSchema
