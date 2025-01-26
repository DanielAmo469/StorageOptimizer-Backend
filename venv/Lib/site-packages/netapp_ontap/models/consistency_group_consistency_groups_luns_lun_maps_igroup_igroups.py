r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupConsistencyGroupsLunsLunMapsIgroupIgroups", "ConsistencyGroupConsistencyGroupsLunsLunMapsIgroupIgroupsSchema"]
__pdoc__ = {
    "ConsistencyGroupConsistencyGroupsLunsLunMapsIgroupIgroupsSchema.resource": False,
    "ConsistencyGroupConsistencyGroupsLunsLunMapsIgroupIgroupsSchema.opts": False,
    "ConsistencyGroupConsistencyGroupsLunsLunMapsIgroupIgroups": False,
}


class ConsistencyGroupConsistencyGroupsLunsLunMapsIgroupIgroupsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupConsistencyGroupsLunsLunMapsIgroupIgroups object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the consistency_group_consistency_groups_luns_lun_maps_igroup_igroups. """

    name = fields.Str(data_key="name")
    r""" The name of the initiator group.


Example: igroup1 """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the initiator group.


Example: 4ea7a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return ConsistencyGroupConsistencyGroupsLunsLunMapsIgroupIgroups

    gettable_fields = [
        "links",
        "name",
        "uuid",
    ]
    """links,name,uuid,"""

    patchable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class ConsistencyGroupConsistencyGroupsLunsLunMapsIgroupIgroups(Resource):

    _schema = ConsistencyGroupConsistencyGroupsLunsLunMapsIgroupIgroupsSchema
