r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NfsServiceExports", "NfsServiceExportsSchema"]
__pdoc__ = {
    "NfsServiceExportsSchema.resource": False,
    "NfsServiceExportsSchema.opts": False,
    "NfsServiceExports": False,
}


class NfsServiceExportsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NfsServiceExports object"""

    name_service_lookup_protocol = fields.Str(data_key="name_service_lookup_protocol")
    r""" Specifies the protocol to use for doing name service lookups.

Valid choices:

* tcp
* udp """

    netgroup_trust_any_nsswitch_no_match = fields.Boolean(data_key="netgroup_trust_any_nsswitch_no_match")
    r""" Specifies if you can consider a no-match result from any of the netgroup ns-switch sources to be authoritative. If this option is enabled, then a no-match response from any of the netgroup ns-switch sources is deemed conclusive even if other sources could not be searched. """

    @property
    def resource(self):
        return NfsServiceExports

    gettable_fields = [
        "name_service_lookup_protocol",
        "netgroup_trust_any_nsswitch_no_match",
    ]
    """name_service_lookup_protocol,netgroup_trust_any_nsswitch_no_match,"""

    patchable_fields = [
        "name_service_lookup_protocol",
        "netgroup_trust_any_nsswitch_no_match",
    ]
    """name_service_lookup_protocol,netgroup_trust_any_nsswitch_no_match,"""

    postable_fields = [
        "name_service_lookup_protocol",
        "netgroup_trust_any_nsswitch_no_match",
    ]
    """name_service_lookup_protocol,netgroup_trust_any_nsswitch_no_match,"""


class NfsServiceExports(Resource):

    _schema = NfsServiceExportsSchema
