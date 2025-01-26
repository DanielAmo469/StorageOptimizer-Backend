r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NodeResponseRecordsManagementInterfaces", "NodeResponseRecordsManagementInterfacesSchema"]
__pdoc__ = {
    "NodeResponseRecordsManagementInterfacesSchema.resource": False,
    "NodeResponseRecordsManagementInterfacesSchema.opts": False,
    "NodeResponseRecordsManagementInterfaces": False,
}


class NodeResponseRecordsManagementInterfacesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NodeResponseRecordsManagementInterfaces object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the node_response_records_management_interfaces. """

    ip = fields.Nested("netapp_ontap.models.application_san_access_iscsi_endpoint_interface_ip.ApplicationSanAccessIscsiEndpointInterfaceIpSchema", unknown=EXCLUDE, data_key="ip")
    r""" The ip field of the node_response_records_management_interfaces. """

    name = fields.Str(data_key="name")
    r""" The name of the interface. If only the name is provided, the SVM scope
must be provided by the object this object is embedded in.


Example: lif1 """

    uuid = fields.Str(data_key="uuid")
    r""" The UUID that uniquely identifies the interface.

Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return NodeResponseRecordsManagementInterfaces

    gettable_fields = [
        "links",
        "ip",
        "name",
        "uuid",
    ]
    """links,ip,name,uuid,"""

    patchable_fields = [
        "ip",
        "name",
        "uuid",
    ]
    """ip,name,uuid,"""

    postable_fields = [
        "ip",
        "name",
        "uuid",
    ]
    """ip,name,uuid,"""


class NodeResponseRecordsManagementInterfaces(Resource):

    _schema = NodeResponseRecordsManagementInterfacesSchema
