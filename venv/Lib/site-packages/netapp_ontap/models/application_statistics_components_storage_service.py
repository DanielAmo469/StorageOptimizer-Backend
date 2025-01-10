r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationStatisticsComponentsStorageService", "ApplicationStatisticsComponentsStorageServiceSchema"]
__pdoc__ = {
    "ApplicationStatisticsComponentsStorageServiceSchema.resource": False,
    "ApplicationStatisticsComponentsStorageServiceSchema.opts": False,
    "ApplicationStatisticsComponentsStorageService": False,
}


class ApplicationStatisticsComponentsStorageServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationStatisticsComponentsStorageService object"""

    name = fields.Str(data_key="name")
    r""" The storage service name. AFF systems support the extreme storage service. All other systems only support value. """

    uuid = fields.Str(data_key="uuid")
    r""" The storage service UUID. """

    @property
    def resource(self):
        return ApplicationStatisticsComponentsStorageService

    gettable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationStatisticsComponentsStorageService(Resource):

    _schema = ApplicationStatisticsComponentsStorageServiceSchema
