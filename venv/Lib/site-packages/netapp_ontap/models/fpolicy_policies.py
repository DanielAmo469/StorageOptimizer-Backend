r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FpolicyPolicies", "FpolicyPoliciesSchema"]
__pdoc__ = {
    "FpolicyPoliciesSchema.resource": False,
    "FpolicyPoliciesSchema.opts": False,
    "FpolicyPolicies": False,
}


class FpolicyPoliciesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FpolicyPolicies object"""

    allow_privileged_access = fields.Boolean(data_key="allow_privileged_access")
    r""" Specifies whether privileged access is required for FPolicy servers.
Privileged access is used when the FPolicy server requires direct
access to the cluster nodes. When this parameter is set to true,
FPolicy servers can access files on the cluster using a separate
data channel with privileged access. """

    enabled = fields.Boolean(data_key="enabled")
    r""" Specifies if the policy is enabled on the SVM or not. If no value is
mentioned for this field but priority is set, then this policy will be enabled. """

    engine = fields.Nested("netapp_ontap.resources.fpolicy_engine.FpolicyEngineSchema", unknown=EXCLUDE, data_key="engine")
    r""" The engine field of the fpolicy_policies. """

    events = fields.List(fields.Nested("netapp_ontap.resources.fpolicy_event.FpolicyEventSchema", unknown=EXCLUDE), data_key="events")
    r""" The events field of the fpolicy_policies.

Example: ["event_nfs_close","event_open"] """

    mandatory = fields.Boolean(data_key="mandatory")
    r""" Specifies what action to take on a file access event in a case when all primary and secondary servers are down or no response is received from the FPolicy servers within a given timeout period. When this parameter is set to true, file access events will be denied under these circumstances. """

    name = fields.Str(data_key="name")
    r""" Specifies the name of the policy.

Example: fp_policy_1 """

    passthrough_read = fields.Boolean(data_key="passthrough_read")
    r""" Specifies whether passthrough-read should be allowed for FPolicy servers
registered for the policy. Passthrough-read is a way to read data for
offline files without restoring the files to primary storage. Offline
files are files that have been moved to secondary storage. """

    priority = Size(data_key="priority")
    r""" Specifies the priority that is assigned to this policy. """

    privileged_user = fields.Str(data_key="privileged_user")
    r""" Specifies the privileged user name for accessing files on the cluster
using a separate data channel with privileged access. The input for
this field should be in "domain\username" format.


Example: mydomain\testuser """

    scope = fields.Nested("netapp_ontap.models.fpolicy_policies_scope.FpolicyPoliciesScopeSchema", unknown=EXCLUDE, data_key="scope")
    r""" The scope field of the fpolicy_policies. """

    @property
    def resource(self):
        return FpolicyPolicies

    gettable_fields = [
        "allow_privileged_access",
        "enabled",
        "engine.links",
        "engine.name",
        "events",
        "mandatory",
        "name",
        "passthrough_read",
        "priority",
        "privileged_user",
        "scope",
    ]
    """allow_privileged_access,enabled,engine.links,engine.name,events,mandatory,name,passthrough_read,priority,privileged_user,scope,"""

    patchable_fields = [
        "allow_privileged_access",
        "enabled",
        "engine.name",
        "events",
        "mandatory",
        "passthrough_read",
        "priority",
        "privileged_user",
        "scope",
    ]
    """allow_privileged_access,enabled,engine.name,events,mandatory,passthrough_read,priority,privileged_user,scope,"""

    postable_fields = [
        "engine.name",
        "events",
        "mandatory",
        "name",
        "passthrough_read",
        "priority",
        "privileged_user",
        "scope",
    ]
    """engine.name,events,mandatory,name,passthrough_read,priority,privileged_user,scope,"""


class FpolicyPolicies(Resource):

    _schema = FpolicyPoliciesSchema
