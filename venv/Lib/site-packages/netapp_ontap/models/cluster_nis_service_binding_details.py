r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNisServiceBindingDetails", "ClusterNisServiceBindingDetailsSchema"]
__pdoc__ = {
    "ClusterNisServiceBindingDetailsSchema.resource": False,
    "ClusterNisServiceBindingDetailsSchema.opts": False,
    "ClusterNisServiceBindingDetails": False,
}


class ClusterNisServiceBindingDetailsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNisServiceBindingDetails object"""

    server = fields.Str(data_key="server")
    r""" Hostname/IP address of the NIS server in the domain. """

    status = fields.Nested("netapp_ontap.models.binding_status.BindingStatusSchema", unknown=EXCLUDE, data_key="status")
    r""" The status field of the cluster_nis_service_binding_details. """

    @property
    def resource(self):
        return ClusterNisServiceBindingDetails

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


class ClusterNisServiceBindingDetails(Resource):

    _schema = ClusterNisServiceBindingDetailsSchema
