r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ExportClientPolicy", "ExportClientPolicySchema"]
__pdoc__ = {
    "ExportClientPolicySchema.resource": False,
    "ExportClientPolicySchema.opts": False,
    "ExportClientPolicy": False,
}


class ExportClientPolicySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ExportClientPolicy object"""

    id = Size(data_key="id")
    r""" Export policy ID """

    @property
    def resource(self):
        return ExportClientPolicy

    gettable_fields = [
        "id",
    ]
    """id,"""

    patchable_fields = [
        "id",
    ]
    """id,"""

    postable_fields = [
        "id",
    ]
    """id,"""


class ExportClientPolicy(Resource):

    _schema = ExportClientPolicySchema
