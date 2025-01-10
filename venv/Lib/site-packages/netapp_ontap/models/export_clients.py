r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ExportClients", "ExportClientsSchema"]
__pdoc__ = {
    "ExportClientsSchema.resource": False,
    "ExportClientsSchema.opts": False,
    "ExportClients": False,
}


class ExportClientsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ExportClients object"""

    match = fields.Str(data_key="match")
    r""" Client Match Hostname, IP Address, Netgroup, or Domain.
You can specify the match as a string value in any of the
          following formats:

* As a hostname; for instance, host1
* As an IPv4 address; for instance, 10.1.12.24
* As an IPv6 address; for instance, fd20:8b1e:b255:4071::100:1
* As an IPv4 address with a subnet mask expressed as a number of bits; for instance, 10.1.12.0/24
* As an IPv6 address with a subnet mask expressed as a number of bits; for instance, fd20:8b1e:b255:4071::/64
* As an IPv4 address with a network mask; for instance, 10.1.16.0/255.255.255.0
* As a netgroup, with the netgroup name preceded by the @ character; for instance, @eng
* As a domain name preceded by the . character; for instance, .example.com


Example: 0.0.0.0/0 """

    @property
    def resource(self):
        return ExportClients

    gettable_fields = [
        "match",
    ]
    """match,"""

    patchable_fields = [
        "match",
    ]
    """match,"""

    postable_fields = [
        "match",
    ]
    """match,"""


class ExportClients(Resource):

    _schema = ExportClientsSchema
