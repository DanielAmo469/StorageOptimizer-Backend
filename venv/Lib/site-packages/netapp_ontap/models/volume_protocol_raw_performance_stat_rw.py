r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeProtocolRawPerformanceStatRw", "VolumeProtocolRawPerformanceStatRwSchema"]
__pdoc__ = {
    "VolumeProtocolRawPerformanceStatRwSchema.resource": False,
    "VolumeProtocolRawPerformanceStatRwSchema.opts": False,
    "VolumeProtocolRawPerformanceStatRw": False,
}


class VolumeProtocolRawPerformanceStatRwSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeProtocolRawPerformanceStatRw object"""

    count = Size(data_key="count")
    r""" Number of operations of the given type performed on this volume.

Example: 1000 """

    total_time = Size(data_key="total_time")
    r""" The raw data component latency in microseconds measured within ONTAP for all operations of the given type.

Example: 200 """

    volume_protocol_latency_histogram_counts = fields.List(Size, data_key="volume_protocol_latency_histogram_counts")
    r""" The volume_protocol_latency_histogram_counts field of the volume_protocol_raw_performance_stat_rw.

Example: [0,0,0,0,0,15,35,100,200,200,300,500,500,500,1000,1000,800,500,500,300,200,50,40,15,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0] """

    volume_protocol_latency_histogram_labels = fields.List(fields.Str, data_key="volume_protocol_latency_histogram_labels")
    r""" Labels for the latency histogram, ranging from <2us to >20s.

Example: ["<2us","<6us","<10us","<14us","<20us","<40us","<60us","<80us","<100us","<200us","<400us","<600us","<800us","<1ms","<2ms","<4ms","<6ms","<8ms","<10ms","<12ms","<14ms","<16ms","<18ms","<20ms","<40ms","<60ms","<80ms","<100ms","<200ms","<400ms","<600ms","<800ms","<1s","<2s","<4s","<6s","<8s","<10s","<20s",">20s"] """

    volume_protocol_size_histogram_counts = fields.List(Size, data_key="volume_protocol_size_histogram_counts")
    r""" The volume_protocol_size_histogram_counts field of the volume_protocol_raw_performance_stat_rw.

Example: [2400,1055,1100,700,500,300,200,100,100,50,50,75,25,0,0] """

    volume_protocol_size_histogram_labels = fields.List(fields.Str, data_key="volume_protocol_size_histogram_labels")
    r""" Labels for the size histogram, ranging from <4KB to >1024KB.

Example: ["<    4KB","=    4KB","<    8KB","=    8KB","<   16KB","=   16KB","<   32KB","=   32KB","<   64KB","=   64KB","<  256KB","=  256KB","< 1024KB","= 1024KB","> 1024KB"] """

    @property
    def resource(self):
        return VolumeProtocolRawPerformanceStatRw

    gettable_fields = [
        "count",
        "total_time",
        "volume_protocol_latency_histogram_counts",
        "volume_protocol_latency_histogram_labels",
        "volume_protocol_size_histogram_counts",
        "volume_protocol_size_histogram_labels",
    ]
    """count,total_time,volume_protocol_latency_histogram_counts,volume_protocol_latency_histogram_labels,volume_protocol_size_histogram_counts,volume_protocol_size_histogram_labels,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class VolumeProtocolRawPerformanceStatRw(Resource):

    _schema = VolumeProtocolRawPerformanceStatRwSchema
