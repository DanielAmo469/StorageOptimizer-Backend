r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationComponentSnapshotSvm", "ApplicationComponentSnapshotSvmSchema"]
__pdoc__ = {
    "ApplicationComponentSnapshotSvmSchema.resource": False,
    "ApplicationComponentSnapshotSvmSchema.opts": False,
    "ApplicationComponentSnapshotSvm": False,
}


class ApplicationComponentSnapshotSvmSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationComponentSnapshotSvm object"""

    name = fields.Str(data_key="name")
    r""" SVM Name """

    uuid = fields.Str(data_key="uuid")
    r""" SVM UUID """

    @property
    def resource(self):
        return ApplicationComponentSnapshotSvm

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


class ApplicationComponentSnapshotSvm(Resource):

    _schema = ApplicationComponentSnapshotSvmSchema
