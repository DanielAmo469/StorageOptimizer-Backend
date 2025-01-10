r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationRpoComponents", "ApplicationRpoComponentsSchema"]
__pdoc__ = {
    "ApplicationRpoComponentsSchema.resource": False,
    "ApplicationRpoComponentsSchema.opts": False,
    "ApplicationRpoComponents": False,
}


class ApplicationRpoComponentsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationRpoComponents object"""

    name = fields.Str(data_key="name")
    r""" Component Name. """

    rpo = fields.Nested("netapp_ontap.models.application_rpo_components_rpo.ApplicationRpoComponentsRpoSchema", unknown=EXCLUDE, data_key="rpo")
    r""" The rpo field of the application_rpo_components. """

    uuid = fields.Str(data_key="uuid")
    r""" Component UUID. """

    @property
    def resource(self):
        return ApplicationRpoComponents

    gettable_fields = [
        "name",
        "rpo",
        "uuid",
    ]
    """name,rpo,uuid,"""

    patchable_fields = [
        "rpo",
    ]
    """rpo,"""

    postable_fields = [
        "rpo",
    ]
    """rpo,"""


class ApplicationRpoComponents(Resource):

    _schema = ApplicationRpoComponentsSchema
