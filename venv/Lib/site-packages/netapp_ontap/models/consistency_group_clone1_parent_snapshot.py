r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupClone1ParentSnapshot", "ConsistencyGroupClone1ParentSnapshotSchema"]
__pdoc__ = {
    "ConsistencyGroupClone1ParentSnapshotSchema.resource": False,
    "ConsistencyGroupClone1ParentSnapshotSchema.opts": False,
    "ConsistencyGroupClone1ParentSnapshot": False,
}


class ConsistencyGroupClone1ParentSnapshotSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupClone1ParentSnapshot object"""

    name = fields.Str(data_key="name")
    r""" Name of an existing Snapshot copy of a parent consistency group. """

    @property
    def resource(self):
        return ConsistencyGroupClone1ParentSnapshot

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class ConsistencyGroupClone1ParentSnapshot(Resource):

    _schema = ConsistencyGroupClone1ParentSnapshotSchema
