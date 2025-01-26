r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsFilterRules", "EmsFilterRulesSchema"]
__pdoc__ = {
    "EmsFilterRulesSchema.resource": False,
    "EmsFilterRulesSchema.opts": False,
    "EmsFilterRules": False,
}


class EmsFilterRulesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsFilterRules object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the ems_filter_rules. """

    index = Size(data_key="index")
    r""" Rule index. Rules are evaluated in ascending order. If a rule's index order is not specified during creation, the rule is appended to the end of the list.

Example: 1 """

    message_criteria = fields.Nested("netapp_ontap.models.ems_filter_rules_message_criteria.EmsFilterRulesMessageCriteriaSchema", unknown=EXCLUDE, data_key="message_criteria")
    r""" The message_criteria field of the ems_filter_rules. """

    parameter_criteria = fields.List(fields.Nested("netapp_ontap.models.ems_parameter_criterion.EmsParameterCriterionSchema", unknown=EXCLUDE), data_key="parameter_criteria")
    r""" Parameter criteria used to match against events' parameters. Each parameter consists of a name and a value. When multiple parameter criteria are provided in a rule, all must match for the rule to be considered matched. A pattern can include one or more wildcard '*' characters. """

    type = fields.Str(data_key="type")
    r""" Rule type

Valid choices:

* include
* exclude """

    @property
    def resource(self):
        return EmsFilterRules

    gettable_fields = [
        "links",
        "index",
        "message_criteria",
        "parameter_criteria",
        "type",
    ]
    """links,index,message_criteria,parameter_criteria,type,"""

    patchable_fields = [
        "index",
        "message_criteria",
        "parameter_criteria",
        "type",
    ]
    """index,message_criteria,parameter_criteria,type,"""

    postable_fields = [
        "index",
        "message_criteria",
        "parameter_criteria",
        "type",
    ]
    """index,message_criteria,parameter_criteria,type,"""


class EmsFilterRules(Resource):

    _schema = EmsFilterRulesSchema
