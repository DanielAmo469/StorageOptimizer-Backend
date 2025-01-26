r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["GroupPolicyObjectRegistrySetting", "GroupPolicyObjectRegistrySettingSchema"]
__pdoc__ = {
    "GroupPolicyObjectRegistrySettingSchema.resource": False,
    "GroupPolicyObjectRegistrySettingSchema.opts": False,
    "GroupPolicyObjectRegistrySetting": False,
}


class GroupPolicyObjectRegistrySettingSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GroupPolicyObjectRegistrySetting object"""

    branchcache = fields.Nested("netapp_ontap.models.group_policy_object_branchcache.GroupPolicyObjectBranchcacheSchema", unknown=EXCLUDE, data_key="branchcache")
    r""" The branchcache field of the group_policy_object_registry_setting. """

    refresh_time_interval = fields.Str(data_key="refresh_time_interval")
    r""" Refresh time interval in ISO-8601 format.

Example: P15M """

    refresh_time_random_offset = fields.Str(data_key="refresh_time_random_offset")
    r""" Random offset in ISO-8601 format.

Example: P1D """

    @property
    def resource(self):
        return GroupPolicyObjectRegistrySetting

    gettable_fields = [
        "branchcache",
        "refresh_time_interval",
        "refresh_time_random_offset",
    ]
    """branchcache,refresh_time_interval,refresh_time_random_offset,"""

    patchable_fields = [
        "branchcache",
        "refresh_time_interval",
        "refresh_time_random_offset",
    ]
    """branchcache,refresh_time_interval,refresh_time_random_offset,"""

    postable_fields = [
        "branchcache",
        "refresh_time_interval",
        "refresh_time_random_offset",
    ]
    """branchcache,refresh_time_interval,refresh_time_random_offset,"""


class GroupPolicyObjectRegistrySetting(Resource):

    _schema = GroupPolicyObjectRegistrySettingSchema
