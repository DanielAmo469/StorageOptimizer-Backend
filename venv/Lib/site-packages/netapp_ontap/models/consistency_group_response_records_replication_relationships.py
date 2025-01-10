r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupResponseRecordsReplicationRelationships", "ConsistencyGroupResponseRecordsReplicationRelationshipsSchema"]
__pdoc__ = {
    "ConsistencyGroupResponseRecordsReplicationRelationshipsSchema.resource": False,
    "ConsistencyGroupResponseRecordsReplicationRelationshipsSchema.opts": False,
    "ConsistencyGroupResponseRecordsReplicationRelationships": False,
}


class ConsistencyGroupResponseRecordsReplicationRelationshipsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupResponseRecordsReplicationRelationships object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the consistency_group_response_records_replication_relationships. """

    is_source = fields.Boolean(data_key="is_source")
    r""" Indicates whether or not this consistency group is the source for replication. """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the SnapMirror relationship.


Example: 02c9e252-41be-11e9-81d5-00a0986138f7 """

    @property
    def resource(self):
        return ConsistencyGroupResponseRecordsReplicationRelationships

    gettable_fields = [
        "links",
        "is_source",
        "uuid",
    ]
    """links,is_source,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ConsistencyGroupResponseRecordsReplicationRelationships(Resource):

    _schema = ConsistencyGroupResponseRecordsReplicationRelationshipsSchema
