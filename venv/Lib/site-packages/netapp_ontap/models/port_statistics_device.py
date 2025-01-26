r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["PortStatisticsDevice", "PortStatisticsDeviceSchema"]
__pdoc__ = {
    "PortStatisticsDeviceSchema.resource": False,
    "PortStatisticsDeviceSchema.opts": False,
    "PortStatisticsDevice": False,
}


class PortStatisticsDeviceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the PortStatisticsDevice object"""

    link_down_count_raw = Size(data_key="link_down_count_raw")
    r""" The number of link state changes from up to down seen on the device.

Example: 3 """

    receive_raw = fields.Nested("netapp_ontap.models.port_statistics_packet_counters.PortStatisticsPacketCountersSchema", unknown=EXCLUDE, data_key="receive_raw")
    r""" The receive_raw field of the port_statistics_device. """

    timestamp = ImpreciseDateTime(data_key="timestamp")
    r""" The timestamp when the device specific counters were collected.

Example: 2017-01-25T11:20:13.000+0000 """

    transmit_raw = fields.Nested("netapp_ontap.models.port_statistics_packet_counters.PortStatisticsPacketCountersSchema", unknown=EXCLUDE, data_key="transmit_raw")
    r""" The transmit_raw field of the port_statistics_device. """

    @property
    def resource(self):
        return PortStatisticsDevice

    gettable_fields = [
        "link_down_count_raw",
        "receive_raw",
        "timestamp",
        "transmit_raw",
    ]
    """link_down_count_raw,receive_raw,timestamp,transmit_raw,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class PortStatisticsDevice(Resource):

    _schema = PortStatisticsDeviceSchema
