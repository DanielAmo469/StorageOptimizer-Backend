r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeRebalancing", "VolumeRebalancingSchema"]
__pdoc__ = {
    "VolumeRebalancingSchema.resource": False,
    "VolumeRebalancingSchema.opts": False,
    "VolumeRebalancing": False,
}


class VolumeRebalancingSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeRebalancing object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the volume_rebalancing. """

    data_moved = Size(data_key="data_moved")
    r""" The amount of data that has been moved in or out of a constituent. A positive value represents data moving into the constituent while a negative value is data moving out of the constituent. """

    engine = fields.Nested("netapp_ontap.models.volume_rebalancing1_engine.VolumeRebalancing1EngineSchema", unknown=EXCLUDE, data_key="engine")
    r""" The engine field of the volume_rebalancing. """

    exclude_snapshots = fields.Boolean(data_key="exclude_snapshots")
    r""" Specifies whether or not to exclude files that are stuck in Snapshot copies during rebalancing operation. When a new capacity rebalancing operation is started on a FlexGroup volume, it uses the current "exclude_snapshots" value. Once the operation is started, any changes to the "exclude_snapshots" value do not affect the currently running capacity rebalancing operation. Only future capacity rebalancing operations will use the new "exclude_snapshots" value. """

    imbalance_percent = Size(data_key="imbalance_percent")
    r""" Represents the percentage the volume is out of balance. """

    imbalance_size = Size(data_key="imbalance_size")
    r""" Represents how much the volume is out of balance, in bytes. """

    max_constituent_imbalance_percent = Size(data_key="max_constituent_imbalance_percent")
    r""" Absolute percentage of the constituent that is most out of balance. This value will update every 30 seconds when rebalancing is not active and every 10 seconds when rebalancing is active. """

    max_file_moves = Size(data_key="max_file_moves")
    r""" Specifies the maximum number of concurrent file moves in a volume capacity rebalancing operation on a constituent of the FlexGroup volume. When a new capacity rebalancing operation is started on a FlexGroup volume, it uses the current "max_file_moves" value. Once the operation is started, any changes to the "max_file_moves" value do not affect the currently running capacity rebalancing operation. Only future capacity rebalancing operations will use the new "max_file_moves" value. """

    max_runtime = fields.Str(data_key="max_runtime")
    r""" This optional field specifies the maximum time a capacity rebalancing operation runs for. Once the maximum runtime has passed, the capacity rebalancing operation stops. If it is not set, the default value is 6 hours. This value cannot be updated while a capacity rebalancing operation is running.  The maximum runtime can be in years, months, days, hours, and minutes. A period specified for years, months, and days is represented in the ISO-8601 format as "P<num>Y", "P<num>M", "P<num>D" respectively, for example "P3D" represents a duration of 3 days. A duration in hours and minutes is represented by "PT<num>H" and "PT<num>M" respectively. """

    max_threshold = Size(data_key="max_threshold")
    r""" Specifies the maximum imbalance percentage for FlexGroup volume constituents. When a constituent's imbalance percentage is larger than this value, files are moved from the constituent. When a new capacity rebalancing operation is started on a FlexGroup volume, it uses the current "max_threshold" value. Once the operation is started, any changes to the "max_threshold" value do not affect the currently running capacity rebalancing operation. Only future capacity rebalancing operations will use the new "max_threshold" value. """

    min_file_size = Size(data_key="min_file_size")
    r""" Specifies the minimum file size to consider for a volume capacity rebalancing operation. When a new capacity rebalancing operation is started on a FlexGroup volume, it uses the current "min_file_size" value. Once the operation is started, any changes to the "min_file_size" value do not affect the currently running capacity rebalancing operation. Only future capacity rebalancing operations will use the new "min_file_size" value. The value must be a multiple of 4KB. If it is not set, the default value is 100MB. Setting "min-file-size" to less than the default value leads to more files being moved. Moved files use granular data, which may impact read/write I/O performance. """

    min_threshold = Size(data_key="min_threshold")
    r""" Specifies the minimum imbalance percentage for FlexGroup volume constituents. When a constituent's imbalance percentage is smaller than this value, files are not moved from the constituent. When a new capacity rebalancing operation is started on a FlexGroup volume, it will use the current "min_threshold" value. Once the operation is started, any changes to the "min_threshold" value do not affect the currently running capacity rebalancing operation. Only future capacity rebalancing operations will use the new "min_threshold" value. """

    notices = fields.List(fields.Nested("netapp_ontap.models.error.ErrorSchema", unknown=EXCLUDE), data_key="notices")
    r""" Capacity rebalancing notice messages. """

    runtime = fields.Str(data_key="runtime")
    r""" Duration the capacity rebalancing operation has been running. """

    start_time = ImpreciseDateTime(data_key="start_time")
    r""" Time when the current capacity rebalancing operation started, or when a future scheduled rebalancing operation begins. """

    state = fields.Str(data_key="state")
    r""" State of the volume capacity rebalancing operation. PATCH the state to "starting" to trigger the capacity rebalance operation, and include start_time to schedule rebalancing. PATCH the state to "stopping" to stop the capacity rebalance operation, or cancel a scheduled rebalancing operation. PATCH without the state with a valid start_time to modify the start_time of an existing scheduled rebalance operation.<br><br>While a FlexGroup volume is rebalancing, every constituent will have a rebalancing engine that can either be scanning the filesystem for space usage and files to move, actively moving files or temporarily doing neither.<br><br>If one or more constituents has a state of "rebalancing_source" or "rebalancing_dest", then files are being moved to rebalance the FlexGroup.<br><br>If no files are being moved, more information about what the rebalancing engine is doing for each constituent is available using the "rebalancing.engine" property.<br><br>The following values apply to FlexGroup volumes.<br>not_running &dash; capacity rebalancing is not running on the volume.<br>starting &dash; used in a PATCH operation to start a capacity rebalancing operation.<br>rebalancing &dash; capacity rebalancing is running on the volume.<br> paused &dash; volume capacity rebalancing is paused on the volume.<br>stopping &dash; used in a PATCH operation to stop a capacity rebalancing operation.<br>unknown &dash; the system was unable to determine the rebalancing state for the volume.<br><br>The following values apply to FlexGroup volume constituents.<br>idle &dash; capacity rebalancing is running on the constituent, however, no active scanning or file movement is currently occurring.<br>scanning &dash; the constituent's file system is being scanned to find files to move and determine free space.<br>rebalancing_source &dash; a file is being moved off of the constituent.<br>rebalancing_dest &dash; a file is being moved to the constituent.<br>not_running &dash; capacity rebalancing is not running on the constituent.<br>unknown &dash; the system was unable to determine the rebalancing state for the constituent.

Valid choices:

* not_running
* starting
* rebalancing
* paused
* stopping
* idle
* scanning
* rebalancing_source
* rebalancing_dest
* unknown """

    stop_time = ImpreciseDateTime(data_key="stop_time")
    r""" Time when the capacity rebalancing operation stopped. """

    target_used = Size(data_key="target_used")
    r""" Represents the ideal used size of each constituent. Calculated by dividing the total FlexGroup volume used size by the number of constituents. """

    used_for_imbalance = Size(data_key="used_for_imbalance")
    r""" Represents the used size of each constituent, as determined by the rebalancing engine. Calculated by subtracting the size used by Snapshot copies, the size of files pending deletion and the size of filesystem metadata from the volume used size. """

    @property
    def resource(self):
        return VolumeRebalancing

    gettable_fields = [
        "links",
        "data_moved",
        "engine",
        "exclude_snapshots",
        "imbalance_percent",
        "imbalance_size",
        "max_constituent_imbalance_percent",
        "max_file_moves",
        "max_runtime",
        "max_threshold",
        "min_file_size",
        "min_threshold",
        "notices",
        "runtime",
        "start_time",
        "state",
        "stop_time",
        "target_used",
        "used_for_imbalance",
    ]
    """links,data_moved,engine,exclude_snapshots,imbalance_percent,imbalance_size,max_constituent_imbalance_percent,max_file_moves,max_runtime,max_threshold,min_file_size,min_threshold,notices,runtime,start_time,state,stop_time,target_used,used_for_imbalance,"""

    patchable_fields = [
        "engine",
        "exclude_snapshots",
        "max_file_moves",
        "max_runtime",
        "max_threshold",
        "min_file_size",
        "min_threshold",
        "start_time",
        "state",
    ]
    """engine,exclude_snapshots,max_file_moves,max_runtime,max_threshold,min_file_size,min_threshold,start_time,state,"""

    postable_fields = [
        "engine",
    ]
    """engine,"""


class VolumeRebalancing(Resource):

    _schema = VolumeRebalancingSchema
