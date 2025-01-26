r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["QuotaReportGroup", "QuotaReportGroupSchema"]
__pdoc__ = {
    "QuotaReportGroupSchema.resource": False,
    "QuotaReportGroupSchema.opts": False,
    "QuotaReportGroup": False,
}


class QuotaReportGroupSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the QuotaReportGroup object"""

    id = fields.Str(data_key="id")
    r""" Quota target group ID """

    name = fields.Str(data_key="name")
    r""" Quota target group name """

    @property
    def resource(self):
        return QuotaReportGroup

    gettable_fields = [
        "id",
        "name",
    ]
    """id,name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class QuotaReportGroup(Resource):

    _schema = QuotaReportGroupSchema
