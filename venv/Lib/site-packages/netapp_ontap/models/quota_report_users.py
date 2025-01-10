r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["QuotaReportUsers", "QuotaReportUsersSchema"]
__pdoc__ = {
    "QuotaReportUsersSchema.resource": False,
    "QuotaReportUsersSchema.opts": False,
    "QuotaReportUsers": False,
}


class QuotaReportUsersSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the QuotaReportUsers object"""

    id = fields.Str(data_key="id")
    r""" Quota target user ID """

    name = fields.Str(data_key="name")
    r""" Quota target user name """

    @property
    def resource(self):
        return QuotaReportUsers

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


class QuotaReportUsers(Resource):

    _schema = QuotaReportUsersSchema
