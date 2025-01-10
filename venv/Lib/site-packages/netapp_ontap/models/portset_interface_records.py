r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["PortsetInterfaceRecords", "PortsetInterfaceRecordsSchema"]
__pdoc__ = {
    "PortsetInterfaceRecordsSchema.resource": False,
    "PortsetInterfaceRecordsSchema.opts": False,
    "PortsetInterfaceRecords": False,
}


class PortsetInterfaceRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the PortsetInterfaceRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the portset_interface_records. """

    fc = fields.Nested("netapp_ontap.resources.fc_interface.FcInterfaceSchema", unknown=EXCLUDE, data_key="fc")
    r""" The fc field of the portset_interface_records. """

    ip = fields.Nested("netapp_ontap.resources.ip_interface.IpInterfaceSchema", unknown=EXCLUDE, data_key="ip")
    r""" The ip field of the portset_interface_records. """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the network interface.


Example: 4ea7a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return PortsetInterfaceRecords

    gettable_fields = [
        "links",
        "fc.links",
        "fc.name",
        "fc.uuid",
        "fc.wwpn",
        "ip.links",
        "ip.ip",
        "ip.name",
        "ip.uuid",
        "uuid",
    ]
    """links,fc.links,fc.name,fc.uuid,fc.wwpn,ip.links,ip.ip,ip.name,ip.uuid,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "fc.name",
        "fc.uuid",
        "ip.ip",
        "ip.name",
        "ip.uuid",
    ]
    """fc.name,fc.uuid,ip.ip,ip.name,ip.uuid,"""


class PortsetInterfaceRecords(Resource):

    _schema = PortsetInterfaceRecordsSchema
