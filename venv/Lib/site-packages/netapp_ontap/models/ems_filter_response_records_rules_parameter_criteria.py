r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsFilterResponseRecordsRulesParameterCriteria", "EmsFilterResponseRecordsRulesParameterCriteriaSchema"]
__pdoc__ = {
    "EmsFilterResponseRecordsRulesParameterCriteriaSchema.resource": False,
    "EmsFilterResponseRecordsRulesParameterCriteriaSchema.opts": False,
    "EmsFilterResponseRecordsRulesParameterCriteria": False,
}


class EmsFilterResponseRecordsRulesParameterCriteriaSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsFilterResponseRecordsRulesParameterCriteria object"""

    name_pattern = fields.Str(data_key="name_pattern")
    r""" Parameter name pattern. Wildcard character '*' is supported.

Example: vol """

    value_pattern = fields.Str(data_key="value_pattern")
    r""" Parameter value pattern. Wildcard character '*' is supported.

Example: cloud* """

    @property
    def resource(self):
        return EmsFilterResponseRecordsRulesParameterCriteria

    gettable_fields = [
        "name_pattern",
        "value_pattern",
    ]
    """name_pattern,value_pattern,"""

    patchable_fields = [
        "name_pattern",
        "value_pattern",
    ]
    """name_pattern,value_pattern,"""

    postable_fields = [
        "name_pattern",
        "value_pattern",
    ]
    """name_pattern,value_pattern,"""


class EmsFilterResponseRecordsRulesParameterCriteria(Resource):

    _schema = EmsFilterResponseRecordsRulesParameterCriteriaSchema
