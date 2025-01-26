r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NisServiceBindingDetails", "NisServiceBindingDetailsSchema"]
__pdoc__ = {
    "NisServiceBindingDetailsSchema.resource": False,
    "NisServiceBindingDetailsSchema.opts": False,
    "NisServiceBindingDetails": False,
}


class NisServiceBindingDetailsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NisServiceBindingDetails object"""

    server = fields.Str(data_key="server")
    r""" Hostname/IP address of the NIS server in the domain. """

    status = fields.Nested("netapp_ontap.models.binding_status.BindingStatusSchema", unknown=EXCLUDE, data_key="status")
    r""" The status field of the nis_service_binding_details. """

    @property
    def resource(self):
        return NisServiceBindingDetails

    gettable_fields = [
        "server",
        "status",
    ]
    """server,status,"""

    patchable_fields = [
        "server",
        "status",
    ]
    """server,status,"""

    postable_fields = [
        "server",
        "status",
    ]
    """server,status,"""


class NisServiceBindingDetails(Resource):

    _schema = NisServiceBindingDetailsSchema
