r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageBridgeSasPorts", "StorageBridgeSasPortsSchema"]
__pdoc__ = {
    "StorageBridgeSasPortsSchema.resource": False,
    "StorageBridgeSasPortsSchema.opts": False,
    "StorageBridgeSasPorts": False,
}


class StorageBridgeSasPortsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageBridgeSasPorts object"""

    cable = fields.Nested("netapp_ontap.models.storage_bridge_sas_ports_cable.StorageBridgeSasPortsCableSchema", unknown=EXCLUDE, data_key="cable")
    r""" The cable field of the storage_bridge_sas_ports. """

    data_rate_capability = fields.Number(data_key="data_rate_capability")
    r""" Bridge SAS port data rate capability, in Gbps """

    enabled = fields.Boolean(data_key="enabled")
    r""" Indicates whether a bridge SAS port is enabled. """

    id = Size(data_key="id")
    r""" Bridge SAS port index """

    negotiated_data_rate = fields.Number(data_key="negotiated_data_rate")
    r""" Bridge SAS port negotiated data rate, in Gbps """

    phy_1 = fields.Nested("netapp_ontap.models.storage_bridge_sas_ports_phy1.StorageBridgeSasPortsPhy1Schema", unknown=EXCLUDE, data_key="phy_1")
    r""" The phy_1 field of the storage_bridge_sas_ports. """

    phy_2 = fields.Nested("netapp_ontap.models.storage_bridge_sas_ports_phy2.StorageBridgeSasPortsPhy2Schema", unknown=EXCLUDE, data_key="phy_2")
    r""" The phy_2 field of the storage_bridge_sas_ports. """

    phy_3 = fields.Nested("netapp_ontap.models.storage_bridge_sas_ports_phy3.StorageBridgeSasPortsPhy3Schema", unknown=EXCLUDE, data_key="phy_3")
    r""" The phy_3 field of the storage_bridge_sas_ports. """

    phy_4 = fields.Nested("netapp_ontap.models.storage_bridge_sas_ports_phy4.StorageBridgeSasPortsPhy4Schema", unknown=EXCLUDE, data_key="phy_4")
    r""" The phy_4 field of the storage_bridge_sas_ports. """

    state = fields.Str(data_key="state")
    r""" Bridge SAS port state

Valid choices:

* error
* online
* offline """

    wwn = fields.Str(data_key="wwn")
    r""" Bridge SAS port world wide name

Example: 2100001086a54100 """

    @property
    def resource(self):
        return StorageBridgeSasPorts

    gettable_fields = [
        "cable",
        "data_rate_capability",
        "enabled",
        "id",
        "negotiated_data_rate",
        "phy_1",
        "phy_2",
        "phy_3",
        "phy_4",
        "state",
        "wwn",
    ]
    """cable,data_rate_capability,enabled,id,negotiated_data_rate,phy_1,phy_2,phy_3,phy_4,state,wwn,"""

    patchable_fields = [
        "cable",
        "data_rate_capability",
        "enabled",
        "id",
        "negotiated_data_rate",
        "phy_1",
        "phy_2",
        "phy_3",
        "phy_4",
        "state",
        "wwn",
    ]
    """cable,data_rate_capability,enabled,id,negotiated_data_rate,phy_1,phy_2,phy_3,phy_4,state,wwn,"""

    postable_fields = [
        "cable",
        "data_rate_capability",
        "enabled",
        "id",
        "negotiated_data_rate",
        "phy_1",
        "phy_2",
        "phy_3",
        "phy_4",
        "state",
        "wwn",
    ]
    """cable,data_rate_capability,enabled,id,negotiated_data_rate,phy_1,phy_2,phy_3,phy_4,state,wwn,"""


class StorageBridgeSasPorts(Resource):

    _schema = StorageBridgeSasPortsSchema
