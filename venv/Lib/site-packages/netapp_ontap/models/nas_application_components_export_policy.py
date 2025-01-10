r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NasApplicationComponentsExportPolicy", "NasApplicationComponentsExportPolicySchema"]
__pdoc__ = {
    "NasApplicationComponentsExportPolicySchema.resource": False,
    "NasApplicationComponentsExportPolicySchema.opts": False,
    "NasApplicationComponentsExportPolicy": False,
}


class NasApplicationComponentsExportPolicySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NasApplicationComponentsExportPolicy object"""

    id = Size(data_key="id")
    r""" The ID of an existing NFS export policy. """

    name = fields.Str(data_key="name")
    r""" The name of an existing NFS export policy. """

    @property
    def resource(self):
        return NasApplicationComponentsExportPolicy

    gettable_fields = [
        "id",
        "name",
    ]
    """id,name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "id",
        "name",
    ]
    """id,name,"""


class NasApplicationComponentsExportPolicy(Resource):

    _schema = NasApplicationComponentsExportPolicySchema
