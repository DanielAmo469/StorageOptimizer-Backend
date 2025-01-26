r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationStatisticsComponentsSnapshot", "ApplicationStatisticsComponentsSnapshotSchema"]
__pdoc__ = {
    "ApplicationStatisticsComponentsSnapshotSchema.resource": False,
    "ApplicationStatisticsComponentsSnapshotSchema.opts": False,
    "ApplicationStatisticsComponentsSnapshot": False,
}


class ApplicationStatisticsComponentsSnapshotSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationStatisticsComponentsSnapshot object"""

    reserve = Size(data_key="reserve")
    r""" The amount of space reserved by the system for Snapshot copies. """

    used = Size(data_key="used")
    r""" The amount of spacing currently in use by the system to store Snapshot copies. """

    @property
    def resource(self):
        return ApplicationStatisticsComponentsSnapshot

    gettable_fields = [
        "reserve",
        "used",
    ]
    """reserve,used,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationStatisticsComponentsSnapshot(Resource):

    _schema = ApplicationStatisticsComponentsSnapshotSchema
