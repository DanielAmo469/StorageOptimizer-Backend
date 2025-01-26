r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupResponseRecordsConsistencyGroupsVolumes", "ConsistencyGroupResponseRecordsConsistencyGroupsVolumesSchema"]
__pdoc__ = {
    "ConsistencyGroupResponseRecordsConsistencyGroupsVolumesSchema.resource": False,
    "ConsistencyGroupResponseRecordsConsistencyGroupsVolumesSchema.opts": False,
    "ConsistencyGroupResponseRecordsConsistencyGroupsVolumes": False,
}


class ConsistencyGroupResponseRecordsConsistencyGroupsVolumesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupResponseRecordsConsistencyGroupsVolumes object"""

    activity_tracking = fields.Nested("netapp_ontap.models.consistency_group_volume_activity_tracking.ConsistencyGroupVolumeActivityTrackingSchema", unknown=EXCLUDE, data_key="activity_tracking")
    r""" The activity_tracking field of the consistency_group_response_records_consistency_groups_volumes. """

    analytics = fields.Nested("netapp_ontap.models.consistency_group_volume_analytics.ConsistencyGroupVolumeAnalyticsSchema", unknown=EXCLUDE, data_key="analytics")
    r""" The analytics field of the consistency_group_response_records_consistency_groups_volumes. """

    comment = fields.Str(data_key="comment")
    r""" A comment for the volume. Valid in POST or PATCH. """

    language = fields.Str(data_key="language")
    r""" Language encoding setting for volume. If no language is specified, the volume inherits its SVM language encoding setting.

Valid choices:

* ar
* ar.utf_8
* c
* c.utf_8
* cs
* cs.utf_8
* da
* da.utf_8
* de
* de.utf_8
* en
* en.utf_8
* en_us
* en_us.utf_8
* es
* es.utf_8
* fi
* fi.utf_8
* fr
* fr.utf_8
* he
* he.utf_8
* hr
* hr.utf_8
* hu
* hu.utf_8
* it
* it.utf_8
* ja
* ja.utf_8
* ja_jp.932
* ja_jp.932.utf_8
* ja_jp.pck
* ja_jp.pck.utf_8
* ja_jp.pck_v2
* ja_jp.pck_v2.utf_8
* ja_v1
* ja_v1.utf_8
* ko
* ko.utf_8
* nl
* nl.utf_8
* no
* no.utf_8
* pl
* pl.utf_8
* pt
* pt.utf_8
* ro
* ro.utf_8
* ru
* ru.utf_8
* sk
* sk.utf_8
* sl
* sl.utf_8
* sv
* sv.utf_8
* tr
* tr.utf_8
* utf8mb4
* zh
* zh.gbk
* zh.gbk.utf_8
* zh.utf_8
* zh_tw
* zh_tw.big5
* zh_tw.big5.utf_8
* zh_tw.utf_8 """

    name = fields.Str(data_key="name")
    r""" Volume name. The name of volume must start with an alphabetic character (a to z or A to Z) or an underscore (_). The name must be 197 or fewer characters in length for FlexGroups, and 203 or fewer characters in length for all other types of volumes. Volume names must be unique within an SVM. Required on POST.

Example: vol_cs_dept """

    nas = fields.Nested("netapp_ontap.models.consistency_group_nas.ConsistencyGroupNasSchema", unknown=EXCLUDE, data_key="nas")
    r""" The nas field of the consistency_group_response_records_consistency_groups_volumes. """

    provisioning_options = fields.Nested("netapp_ontap.models.consistency_group_volume_provisioning_options.ConsistencyGroupVolumeProvisioningOptionsSchema", unknown=EXCLUDE, data_key="provisioning_options")
    r""" The provisioning_options field of the consistency_group_response_records_consistency_groups_volumes. """

    qos = fields.Nested("netapp_ontap.models.consistency_group_qos.ConsistencyGroupQosSchema", unknown=EXCLUDE, data_key="qos")
    r""" The qos field of the consistency_group_response_records_consistency_groups_volumes. """

    snapshot_policy = fields.Nested("netapp_ontap.resources.snapshot_policy.SnapshotPolicySchema", unknown=EXCLUDE, data_key="snapshot_policy")
    r""" The Snapshot copy policy for this volume. """

    space = fields.Nested("netapp_ontap.models.consistency_group_volume_space.ConsistencyGroupVolumeSpaceSchema", unknown=EXCLUDE, data_key="space")
    r""" The space field of the consistency_group_response_records_consistency_groups_volumes. """

    tiering = fields.Nested("netapp_ontap.models.consistency_group_tiering.ConsistencyGroupTieringSchema", unknown=EXCLUDE, data_key="tiering")
    r""" The tiering field of the consistency_group_response_records_consistency_groups_volumes. """

    uuid = fields.Str(data_key="uuid")
    r""" Unique identifier for the volume. This corresponds to the instance-uuid that is exposed in the CLI and ONTAPI. It does not change due to a volume move.

Example: 028baa66-41bd-11e9-81d5-00a0986138f7 """

    @property
    def resource(self):
        return ConsistencyGroupResponseRecordsConsistencyGroupsVolumes

    gettable_fields = [
        "activity_tracking",
        "analytics",
        "comment",
        "language",
        "name",
        "nas",
        "qos",
        "snapshot_policy.links",
        "snapshot_policy.name",
        "snapshot_policy.uuid",
        "space",
        "tiering",
        "uuid",
    ]
    """activity_tracking,analytics,comment,language,name,nas,qos,snapshot_policy.links,snapshot_policy.name,snapshot_policy.uuid,space,tiering,uuid,"""

    patchable_fields = [
        "activity_tracking",
        "analytics",
        "comment",
        "name",
        "nas",
        "provisioning_options",
        "qos",
        "snapshot_policy.name",
        "snapshot_policy.uuid",
        "space",
        "tiering",
    ]
    """activity_tracking,analytics,comment,name,nas,provisioning_options,qos,snapshot_policy.name,snapshot_policy.uuid,space,tiering,"""

    postable_fields = [
        "activity_tracking",
        "analytics",
        "comment",
        "language",
        "name",
        "nas",
        "provisioning_options",
        "qos",
        "snapshot_policy.name",
        "snapshot_policy.uuid",
        "space",
        "tiering",
    ]
    """activity_tracking,analytics,comment,language,name,nas,provisioning_options,qos,snapshot_policy.name,snapshot_policy.uuid,space,tiering,"""


class ConsistencyGroupResponseRecordsConsistencyGroupsVolumes(Resource):

    _schema = ConsistencyGroupResponseRecordsConsistencyGroupsVolumesSchema
