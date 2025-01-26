r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsRoleConfigResponseRecords", "EmsRoleConfigResponseRecordsSchema"]
__pdoc__ = {
    "EmsRoleConfigResponseRecordsSchema.resource": False,
    "EmsRoleConfigResponseRecordsSchema.opts": False,
    "EmsRoleConfigResponseRecords": False,
}


class EmsRoleConfigResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsRoleConfigResponseRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the ems_role_config_response_records. """

    access_control_role = fields.Nested("netapp_ontap.resources.role.RoleSchema", unknown=EXCLUDE, data_key="access_control_role")
    r""" The access_control_role field of the ems_role_config_response_records. """

    event_filter = fields.Nested("netapp_ontap.resources.ems_filter.EmsFilterSchema", unknown=EXCLUDE, data_key="event_filter")
    r""" The event_filter field of the ems_role_config_response_records. """

    limit_access_to_global_configs = fields.Boolean(data_key="limit_access_to_global_configs")
    r""" Indicates whether the access control has limited access to global EMS configurations. """

    @property
    def resource(self):
        return EmsRoleConfigResponseRecords

    gettable_fields = [
        "links",
        "access_control_role.links",
        "access_control_role.name",
        "event_filter.links",
        "event_filter.name",
        "limit_access_to_global_configs",
    ]
    """links,access_control_role.links,access_control_role.name,event_filter.links,event_filter.name,limit_access_to_global_configs,"""

    patchable_fields = [
        "event_filter.name",
        "limit_access_to_global_configs",
    ]
    """event_filter.name,limit_access_to_global_configs,"""

    postable_fields = [
        "access_control_role.name",
        "event_filter.name",
        "limit_access_to_global_configs",
    ]
    """access_control_role.name,event_filter.name,limit_access_to_global_configs,"""


class EmsRoleConfigResponseRecords(Resource):

    _schema = EmsRoleConfigResponseRecordsSchema
