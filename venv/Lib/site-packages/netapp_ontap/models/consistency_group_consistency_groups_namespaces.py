r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupConsistencyGroupsNamespaces", "ConsistencyGroupConsistencyGroupsNamespacesSchema"]
__pdoc__ = {
    "ConsistencyGroupConsistencyGroupsNamespacesSchema.resource": False,
    "ConsistencyGroupConsistencyGroupsNamespacesSchema.opts": False,
    "ConsistencyGroupConsistencyGroupsNamespaces": False,
}


class ConsistencyGroupConsistencyGroupsNamespacesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupConsistencyGroupsNamespaces object"""

    auto_delete = fields.Boolean(data_key="auto_delete")
    r""" This property marks the NVMe namespace for auto deletion when the volume containing the namespace runs out of space. This is most commonly set on namespace clones.<br/>
When set to _true_, the NVMe namespace becomes eligible for automatic deletion when the volume runs out of space. Auto deletion only occurs when the volume containing the namespace is also configured for auto deletion and free space in the volume decreases below a particular threshold.<br/>
This property is optional in POST and PATCH. The default value for a new NVMe namespace is _false_.<br/>
There is an added computational cost to retrieving this property's value. It is not populated for either a collection GET or an instance GET unless it is explicitly requested using the `fields` query parameter. See [`Requesting specific fields`](#Requesting_specific_fields) to learn more. """

    comment = fields.Str(data_key="comment")
    r""" A configurable comment available for use by the administrator. Valid in POST and PATCH. """

    create_time = ImpreciseDateTime(data_key="create_time")
    r""" The time the NVMe namespace was created.

Example: 2018-06-04T19:00:00.000+0000 """

    enabled = fields.Boolean(data_key="enabled")
    r""" The enabled state of the NVMe namespace. Certain error conditions cause the namespace to become disabled. If the namespace is disabled, you can check the `state` property to determine what error disabled the namespace. An NVMe namespace is enabled automatically when it is created. """

    name = fields.Str(data_key="name")
    r""" The fully qualified path name of the NVMe namespace composed of a "/vol" prefix, the volume name, the (optional) qtree name and base name of the namespace. Valid in POST.<br/>
NVMe namespaces do not support rename, or movement between volumes.


Example: /vol/volume1/qtree1/namespace1 """

    os_type = fields.Str(data_key="os_type")
    r""" The operating system type of the NVMe namespace.<br/>
Required in POST when creating an NVMe namespace that is not a clone of another. Disallowed in POST when creating a namespace clone.


Valid choices:

* aix
* linux
* vmware
* windows """

    provisioning_options = fields.Nested("netapp_ontap.models.consistency_group_vdisk_provisioning_options.ConsistencyGroupVdiskProvisioningOptionsSchema", unknown=EXCLUDE, data_key="provisioning_options")
    r""" The provisioning_options field of the consistency_group_consistency_groups_namespaces. """

    space = fields.Nested("netapp_ontap.models.consistency_group_namespace_space.ConsistencyGroupNamespaceSpaceSchema", unknown=EXCLUDE, data_key="space")
    r""" The space field of the consistency_group_consistency_groups_namespaces. """

    status = fields.Nested("netapp_ontap.models.consistency_group_namespace_status.ConsistencyGroupNamespaceStatusSchema", unknown=EXCLUDE, data_key="status")
    r""" The status field of the consistency_group_consistency_groups_namespaces. """

    subsystem_map = fields.Nested("netapp_ontap.models.consistency_group_namespace_subsystem_map.ConsistencyGroupNamespaceSubsystemMapSchema", unknown=EXCLUDE, data_key="subsystem_map")
    r""" The subsystem_map field of the consistency_group_consistency_groups_namespaces. """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the NVMe namespace.


Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return ConsistencyGroupConsistencyGroupsNamespaces

    gettable_fields = [
        "auto_delete",
        "comment",
        "create_time",
        "enabled",
        "name",
        "os_type",
        "space",
        "status",
        "subsystem_map",
        "uuid",
    ]
    """auto_delete,comment,create_time,enabled,name,os_type,space,status,subsystem_map,uuid,"""

    patchable_fields = [
        "auto_delete",
        "comment",
        "provisioning_options",
        "space",
        "subsystem_map",
    ]
    """auto_delete,comment,provisioning_options,space,subsystem_map,"""

    postable_fields = [
        "auto_delete",
        "comment",
        "name",
        "os_type",
        "provisioning_options",
        "space",
        "subsystem_map",
    ]
    """auto_delete,comment,name,os_type,provisioning_options,space,subsystem_map,"""


class ConsistencyGroupConsistencyGroupsNamespaces(Resource):

    _schema = ConsistencyGroupConsistencyGroupsNamespacesSchema
