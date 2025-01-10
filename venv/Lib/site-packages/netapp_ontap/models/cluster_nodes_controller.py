r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesController", "ClusterNodesControllerSchema"]
__pdoc__ = {
    "ClusterNodesControllerSchema.resource": False,
    "ClusterNodesControllerSchema.opts": False,
    "ClusterNodesController": False,
}


class ClusterNodesControllerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesController object"""

    board = fields.Str(data_key="board")
    r""" Type of the system board. This is defined by vendor.

Example: System Board XXVIII """

    cpu = fields.Nested("netapp_ontap.models.cluster_nodes_controller_cpu.ClusterNodesControllerCpuSchema", unknown=EXCLUDE, data_key="cpu")
    r""" The cpu field of the cluster_nodes_controller. """

    failed_fan = fields.Nested("netapp_ontap.models.cluster_nodes_controller_failed_fan.ClusterNodesControllerFailedFanSchema", unknown=EXCLUDE, data_key="failed_fan")
    r""" The failed_fan field of the cluster_nodes_controller. """

    failed_power_supply = fields.Nested("netapp_ontap.models.cluster_nodes_controller_failed_power_supply.ClusterNodesControllerFailedPowerSupplySchema", unknown=EXCLUDE, data_key="failed_power_supply")
    r""" The failed_power_supply field of the cluster_nodes_controller. """

    flash_cache = fields.List(fields.Nested("netapp_ontap.models.cluster_nodes_controller_flash_cache.ClusterNodesControllerFlashCacheSchema", unknown=EXCLUDE), data_key="flash_cache")
    r""" A list of Flash-Cache devices. Only returned when requested by name. """

    frus = fields.List(fields.Nested("netapp_ontap.models.cluster_nodes_controller_frus.ClusterNodesControllerFrusSchema", unknown=EXCLUDE), data_key="frus")
    r""" List of FRUs on the node. Only returned when requested by name. """

    memory_size = Size(data_key="memory_size")
    r""" Memory available on the node, in bytes.

Example: 1024000000 """

    over_temperature = fields.Str(data_key="over_temperature")
    r""" Specifies whether the hardware is currently operating outside of its recommended temperature range. The hardware shuts down if the temperature exceeds critical thresholds.

Valid choices:

* over
* normal """

    @property
    def resource(self):
        return ClusterNodesController

    gettable_fields = [
        "board",
        "cpu",
        "failed_fan",
        "failed_power_supply",
        "flash_cache",
        "frus",
        "memory_size",
        "over_temperature",
    ]
    """board,cpu,failed_fan,failed_power_supply,flash_cache,frus,memory_size,over_temperature,"""

    patchable_fields = [
        "cpu",
        "failed_fan",
        "failed_power_supply",
        "frus",
    ]
    """cpu,failed_fan,failed_power_supply,frus,"""

    postable_fields = [
        "cpu",
        "failed_fan",
        "failed_power_supply",
        "frus",
    ]
    """cpu,failed_fan,failed_power_supply,frus,"""


class ClusterNodesController(Resource):

    _schema = ClusterNodesControllerSchema
