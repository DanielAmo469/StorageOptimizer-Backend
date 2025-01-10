r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeStatisticsFlexcacheRaw", "VolumeStatisticsFlexcacheRawSchema"]
__pdoc__ = {
    "VolumeStatisticsFlexcacheRawSchema.resource": False,
    "VolumeStatisticsFlexcacheRawSchema.opts": False,
    "VolumeStatisticsFlexcacheRaw": False,
}


class VolumeStatisticsFlexcacheRawSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeStatisticsFlexcacheRaw object"""

    cache_miss_blocks = Size(data_key="cache_miss_blocks")
    r""" Blocks retrieved from origin in case of a cache miss. This can be divided by the raw client_requested_blocks and multiplied by 100 to calculate the cache miss percentage.

Example: 10 """

    client_requested_blocks = Size(data_key="client_requested_blocks")
    r""" Total blocks requested by the client.

Example: 500 """

    status = fields.Str(data_key="status")
    r""" Errors associated with the sample. For example, if the aggregation of data over multiple nodes fails, then any partial errors might return "ok" on success or "error" on an internal uncategorized failure. Whenever a sample collection is missed but done at a later time, it is back filled to the previous 15 second timestamp and tagged with "backfilled_data". "Inconsistent_ delta_time" is encountered when the time between two collections is not the same for all nodes. Therefore, the aggregated value might be over or under inflated. "Negative_delta" is returned when an expected monotonically increasing value has decreased in value. "Inconsistent_old_data" is returned when one or more nodes do not have the latest data.

Valid choices:

* ok
* error
* partial_no_data
* partial_no_uuid
* partial_no_response
* partial_other_error
* negative_delta
* backfilled_data
* inconsistent_delta_time
* inconsistent_old_data """

    timestamp = ImpreciseDateTime(data_key="timestamp")
    r""" The timestamp of the performance data.

Example: 2017-01-25T11:20:13.000+0000 """

    @property
    def resource(self):
        return VolumeStatisticsFlexcacheRaw

    gettable_fields = [
        "cache_miss_blocks",
        "client_requested_blocks",
        "status",
        "timestamp",
    ]
    """cache_miss_blocks,client_requested_blocks,status,timestamp,"""

    patchable_fields = [
        "cache_miss_blocks",
        "client_requested_blocks",
    ]
    """cache_miss_blocks,client_requested_blocks,"""

    postable_fields = [
        "cache_miss_blocks",
        "client_requested_blocks",
    ]
    """cache_miss_blocks,client_requested_blocks,"""


class VolumeStatisticsFlexcacheRaw(Resource):

    _schema = VolumeStatisticsFlexcacheRawSchema
