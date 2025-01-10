r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupSnapshotResponseRecords", "ConsistencyGroupSnapshotResponseRecordsSchema"]
__pdoc__ = {
    "ConsistencyGroupSnapshotResponseRecordsSchema.resource": False,
    "ConsistencyGroupSnapshotResponseRecordsSchema.opts": False,
    "ConsistencyGroupSnapshotResponseRecords": False,
}


class ConsistencyGroupSnapshotResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupSnapshotResponseRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the consistency_group_snapshot_response_records. """

    comment = fields.Str(data_key="comment")
    r""" Comment for the Snapshot copy.


Example: My Snapshot copy comment """

    consistency_group = fields.Nested("netapp_ontap.resources.consistency_group.ConsistencyGroupSchema", unknown=EXCLUDE, data_key="consistency_group")
    r""" The consistency_group field of the consistency_group_snapshot_response_records. """

    consistency_type = fields.Str(data_key="consistency_type")
    r""" Consistency type. This is for categorization purposes only. A Snapshot copy should not be set to 'application consistent' unless the host application is quiesced for the Snapshot copy. Valid in POST.


Valid choices:

* crash
* application """

    create_time = ImpreciseDateTime(data_key="create_time")
    r""" Time the snapshot copy was created


Example: 2020-10-25T11:20:00.000+0000 """

    is_partial = fields.Boolean(data_key="is_partial")
    r""" Indicates whether the Snapshot copy taken is partial or not.


Example: false """

    missing_volumes = fields.List(fields.Nested("netapp_ontap.resources.volume.VolumeSchema", unknown=EXCLUDE), data_key="missing_volumes")
    r""" List of volumes which are not in the Snapshot copy. """

    name = fields.Str(data_key="name")
    r""" Name of the Snapshot copy. """

    snapmirror_label = fields.Str(data_key="snapmirror_label")
    r""" Snapmirror Label for the Snapshot copy.


Example: sm_label """

    snapshot_volumes = fields.List(fields.Nested("netapp_ontap.models.consistency_group_volume_snapshot.ConsistencyGroupVolumeSnapshotSchema", unknown=EXCLUDE), data_key="snapshot_volumes")
    r""" List of volume and snapshot identifiers for each volume in the Snapshot copy. """

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", unknown=EXCLUDE, data_key="svm")
    r""" The SVM in which the consistency group is located. """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the Snapshot copy. The UUID is generated
by ONTAP when the Snapshot copy is created.


Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return ConsistencyGroupSnapshotResponseRecords

    gettable_fields = [
        "links",
        "comment",
        "consistency_group.links",
        "consistency_group.name",
        "consistency_group.uuid",
        "consistency_type",
        "create_time",
        "is_partial",
        "missing_volumes",
        "name",
        "snapmirror_label",
        "snapshot_volumes",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "uuid",
    ]
    """links,comment,consistency_group.links,consistency_group.name,consistency_group.uuid,consistency_type,create_time,is_partial,missing_volumes,name,snapmirror_label,snapshot_volumes,svm.links,svm.name,svm.uuid,uuid,"""

    patchable_fields = [
        "consistency_group.links",
        "consistency_group.name",
        "consistency_group.uuid",
        "consistency_type",
        "name",
        "svm.name",
        "svm.uuid",
    ]
    """consistency_group.links,consistency_group.name,consistency_group.uuid,consistency_type,name,svm.name,svm.uuid,"""

    postable_fields = [
        "comment",
        "consistency_group.links",
        "consistency_group.name",
        "consistency_group.uuid",
        "consistency_type",
        "name",
        "snapmirror_label",
        "svm.name",
        "svm.uuid",
    ]
    """comment,consistency_group.links,consistency_group.name,consistency_group.uuid,consistency_type,name,snapmirror_label,svm.name,svm.uuid,"""


class ConsistencyGroupSnapshotResponseRecords(Resource):

    _schema = ConsistencyGroupSnapshotResponseRecordsSchema
