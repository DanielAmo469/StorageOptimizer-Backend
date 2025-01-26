r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationSnapshotApplication", "ApplicationSnapshotApplicationSchema"]
__pdoc__ = {
    "ApplicationSnapshotApplicationSchema.resource": False,
    "ApplicationSnapshotApplicationSchema.opts": False,
    "ApplicationSnapshotApplication": False,
}


class ApplicationSnapshotApplicationSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationSnapshotApplication object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the application_snapshot_application. """

    name = fields.Str(data_key="name")
    r""" Application name """

    uuid = fields.Str(data_key="uuid")
    r""" The application UUID. Valid in URL. """

    @property
    def resource(self):
        return ApplicationSnapshotApplication

    gettable_fields = [
        "links",
        "name",
        "uuid",
    ]
    """links,name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationSnapshotApplication(Resource):

    _schema = ApplicationSnapshotApplicationSchema
