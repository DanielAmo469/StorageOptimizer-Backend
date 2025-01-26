r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeProtocolRawPerformanceStats", "VolumeProtocolRawPerformanceStatsSchema"]
__pdoc__ = {
    "VolumeProtocolRawPerformanceStatsSchema.resource": False,
    "VolumeProtocolRawPerformanceStatsSchema.opts": False,
    "VolumeProtocolRawPerformanceStats": False,
}


class VolumeProtocolRawPerformanceStatsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeProtocolRawPerformanceStats object"""

    access = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="access")
    r""" The access field of the volume_protocol_raw_performance_stats. """

    audit = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="audit")
    r""" The audit field of the volume_protocol_raw_performance_stats. """

    create = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_create.VolumeProtocolRawPerformanceStatCreateSchema", unknown=EXCLUDE, data_key="create")
    r""" The create field of the volume_protocol_raw_performance_stats. """

    getattr = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="getattr")
    r""" The getattr field of the volume_protocol_raw_performance_stats. """

    link = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="link")
    r""" The link field of the volume_protocol_raw_performance_stats. """

    lock = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="lock")
    r""" The lock field of the volume_protocol_raw_performance_stats. """

    lookup = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="lookup")
    r""" The lookup field of the volume_protocol_raw_performance_stats. """

    open = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="open")
    r""" The open field of the volume_protocol_raw_performance_stats. """

    read = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_rw.VolumeProtocolRawPerformanceStatRwSchema", unknown=EXCLUDE, data_key="read")
    r""" The read field of the volume_protocol_raw_performance_stats. """

    readdir = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="readdir")
    r""" The readdir field of the volume_protocol_raw_performance_stats. """

    readlink = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="readlink")
    r""" The readlink field of the volume_protocol_raw_performance_stats. """

    rename = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="rename")
    r""" The rename field of the volume_protocol_raw_performance_stats. """

    setattr = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="setattr")
    r""" The setattr field of the volume_protocol_raw_performance_stats. """

    unlink = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="unlink")
    r""" The unlink field of the volume_protocol_raw_performance_stats. """

    watch = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_other.VolumeProtocolRawPerformanceStatOtherSchema", unknown=EXCLUDE, data_key="watch")
    r""" The watch field of the volume_protocol_raw_performance_stats. """

    write = fields.Nested("netapp_ontap.models.volume_protocol_raw_performance_stat_rw.VolumeProtocolRawPerformanceStatRwSchema", unknown=EXCLUDE, data_key="write")
    r""" The write field of the volume_protocol_raw_performance_stats. """

    @property
    def resource(self):
        return VolumeProtocolRawPerformanceStats

    gettable_fields = [
        "access",
        "audit",
        "create",
        "getattr",
        "link",
        "lock",
        "lookup",
        "open",
        "read",
        "readdir",
        "readlink",
        "rename",
        "setattr",
        "unlink",
        "watch",
        "write",
    ]
    """access,audit,create,getattr,link,lock,lookup,open,read,readdir,readlink,rename,setattr,unlink,watch,write,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class VolumeProtocolRawPerformanceStats(Resource):

    _schema = VolumeProtocolRawPerformanceStatsSchema
