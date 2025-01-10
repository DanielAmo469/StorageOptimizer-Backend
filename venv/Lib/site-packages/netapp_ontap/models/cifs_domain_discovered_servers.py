r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CifsDomainDiscoveredServers", "CifsDomainDiscoveredServersSchema"]
__pdoc__ = {
    "CifsDomainDiscoveredServersSchema.resource": False,
    "CifsDomainDiscoveredServersSchema.opts": False,
    "CifsDomainDiscoveredServers": False,
}


class CifsDomainDiscoveredServersSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CifsDomainDiscoveredServers object"""

    domain = fields.Str(data_key="domain")
    r""" Fully Qualified Domain Name.


Example: test.com """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the cifs_domain_discovered_servers. """

    preference = fields.Str(data_key="preference")
    r""" Server Preference


Valid choices:

* unknown
* preferred
* favored
* adequate """

    server_ip = fields.Str(data_key="server_ip")
    r""" Server IP address """

    server_name = fields.Str(data_key="server_name")
    r""" Server Name """

    server_type = fields.Str(data_key="server_type")
    r""" Server Type


Valid choices:

* unknown
* kerberos
* ms_ldap
* ms_dc
* ldap """

    state = fields.Str(data_key="state")
    r""" Server status


Valid choices:

* ok
* unavailable
* slow
* expired
* undetermined
* unreachable """

    @property
    def resource(self):
        return CifsDomainDiscoveredServers

    gettable_fields = [
        "domain",
        "node.links",
        "node.name",
        "node.uuid",
        "preference",
        "server_ip",
        "server_name",
        "server_type",
        "state",
    ]
    """domain,node.links,node.name,node.uuid,preference,server_ip,server_name,server_type,state,"""

    patchable_fields = [
        "domain",
        "node.name",
        "node.uuid",
        "preference",
        "server_ip",
        "server_name",
        "server_type",
        "state",
    ]
    """domain,node.name,node.uuid,preference,server_ip,server_name,server_type,state,"""

    postable_fields = [
        "domain",
        "node.name",
        "node.uuid",
        "preference",
        "server_ip",
        "server_name",
        "server_type",
        "state",
    ]
    """domain,node.name,node.uuid,preference,server_ip,server_name,server_type,state,"""


class CifsDomainDiscoveredServers(Resource):

    _schema = CifsDomainDiscoveredServersSchema
