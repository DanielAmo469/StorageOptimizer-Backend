r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LdapStatus", "LdapStatusSchema"]
__pdoc__ = {
    "LdapStatusSchema.resource": False,
    "LdapStatusSchema.opts": False,
    "LdapStatus": False,
}


class LdapStatusSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LdapStatus object"""

    code = Size(data_key="code")
    r""" Code corresponding to the error message. If there is no error, it will be 0 to indicate success.


Example: 65537300 """

    dn_message = fields.List(fields.Str, data_key="dn_message")
    r""" The dn_message field of the ldap_status. """

    ipv4_state = fields.Str(data_key="ipv4_state")
    r""" The status of the LDAP service with IPv4 address.


Valid choices:

* up
* down """

    ipv6_state = fields.Str(data_key="ipv6_state")
    r""" The status of the LDAP service with IPv6 address.


Valid choices:

* up
* down """

    message = fields.Str(data_key="message")
    r""" Provides additional details on the error if `ipv4_state` or `ipv6_state` is down.
If both `ipv4_state` and `ipv6_state` are up, it provides details of the IP addresses that are connected successfully. """

    state = fields.Str(data_key="state")
    r""" The status of the LDAP service for the SVM. The LDAP service is up if either `ipv4_state` or `ipv6_state` is up.
The LDAP service is down if both `ipv4_state` and `ipv6_state` are down.


Valid choices:

* up
* down """

    @property
    def resource(self):
        return LdapStatus

    gettable_fields = [
        "code",
        "dn_message",
        "ipv4_state",
        "ipv6_state",
        "message",
        "state",
    ]
    """code,dn_message,ipv4_state,ipv6_state,message,state,"""

    patchable_fields = [
        "code",
        "dn_message",
        "ipv4_state",
        "ipv6_state",
        "message",
        "state",
    ]
    """code,dn_message,ipv4_state,ipv6_state,message,state,"""

    postable_fields = [
        "code",
        "dn_message",
        "ipv4_state",
        "ipv6_state",
        "message",
        "state",
    ]
    """code,dn_message,ipv4_state,ipv6_state,message,state,"""


class LdapStatus(Resource):

    _schema = LdapStatusSchema
