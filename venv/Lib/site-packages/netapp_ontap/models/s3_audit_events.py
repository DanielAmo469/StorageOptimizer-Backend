r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3AuditEvents", "S3AuditEventsSchema"]
__pdoc__ = {
    "S3AuditEventsSchema.resource": False,
    "S3AuditEventsSchema.opts": False,
    "S3AuditEvents": False,
}


class S3AuditEventsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3AuditEvents object"""

    data = fields.Boolean(data_key="data")
    r""" Data events """

    management = fields.Boolean(data_key="management")
    r""" Management events """

    @property
    def resource(self):
        return S3AuditEvents

    gettable_fields = [
        "data",
        "management",
    ]
    """data,management,"""

    patchable_fields = [
        "data",
        "management",
    ]
    """data,management,"""

    postable_fields = [
        "data",
        "management",
    ]
    """data,management,"""


class S3AuditEvents(Resource):

    _schema = S3AuditEventsSchema
