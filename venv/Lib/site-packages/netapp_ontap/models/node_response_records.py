r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NodeResponseRecords", "NodeResponseRecordsSchema"]
__pdoc__ = {
    "NodeResponseRecordsSchema.resource": False,
    "NodeResponseRecordsSchema.opts": False,
    "NodeResponseRecords": False,
}


class NodeResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NodeResponseRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the node_response_records. """

    cluster_interface = fields.Nested("netapp_ontap.models.cluster_nodes_cluster_interface.ClusterNodesClusterInterfaceSchema", unknown=EXCLUDE, data_key="cluster_interface")
    r""" The cluster_interface field of the node_response_records. """

    cluster_interfaces = fields.List(fields.Nested("netapp_ontap.models.node_management_interfaces.NodeManagementInterfacesSchema", unknown=EXCLUDE), data_key="cluster_interfaces")
    r""" The cluster_interfaces field of the node_response_records. """

    controller = fields.Nested("netapp_ontap.models.cluster_nodes_controller.ClusterNodesControllerSchema", unknown=EXCLUDE, data_key="controller")
    r""" The controller field of the node_response_records. """

    date = ImpreciseDateTime(data_key="date")
    r""" The current or "wall clock" time of the node in ISO-8601 date, time, and time zone format.
The ISO-8601 date and time are localized based on the ONTAP cluster's timezone setting.


Example: 2019-04-17T15:49:26.000+0000 """

    external_cache = fields.Nested("netapp_ontap.models.cluster_nodes_external_cache.ClusterNodesExternalCacheSchema", unknown=EXCLUDE, data_key="external_cache")
    r""" The external_cache field of the node_response_records. """

    ha = fields.Nested("netapp_ontap.models.node_response_records_ha.NodeResponseRecordsHaSchema", unknown=EXCLUDE, data_key="ha")
    r""" The ha field of the node_response_records. """

    hw_assist = fields.Nested("netapp_ontap.models.hw_assist.HwAssistSchema", unknown=EXCLUDE, data_key="hw_assist")
    r""" The hw_assist field of the node_response_records. """

    is_spares_low = fields.Boolean(data_key="is_spares_low")
    r""" Specifies whether or not the node is in spares low condition. """

    location = fields.Str(data_key="location")
    r""" The location field of the node_response_records.

Example: rack 2 row 5 """

    management_interface = fields.Nested("netapp_ontap.models.cluster_nodes_management_interface.ClusterNodesManagementInterfaceSchema", unknown=EXCLUDE, data_key="management_interface")
    r""" The management_interface field of the node_response_records. """

    management_interfaces = fields.List(fields.Nested("netapp_ontap.models.node_management_interfaces.NodeManagementInterfacesSchema", unknown=EXCLUDE), data_key="management_interfaces")
    r""" The management_interfaces field of the node_response_records. """

    membership = fields.Str(data_key="membership")
    r""" Possible values:

* <i>available</i> - A node is detected on the internal cluster network and can be added to the cluster.  Nodes that have a membership of "available" are not returned when a GET request is called when the cluster exists. Provide a query on the "membership" property for <i>available</i> to scan for nodes on the cluster network. Nodes that have a membership of "available" are returned automatically before a cluster is created.
* <i>joining</i> - Joining nodes are in the process of being added to the cluster. The node might be progressing through the steps to become a member or might have failed. The job to add the node or create the cluster provides details on the current progress of the node.
* <i>member</i> - Nodes that are members have successfully joined the cluster.


Valid choices:

* available
* joining
* member """

    metric = fields.Nested("netapp_ontap.resources.node_metrics.NodeMetricsSchema", unknown=EXCLUDE, data_key="metric")
    r""" The metric field of the node_response_records. """

    metrocluster = fields.Nested("netapp_ontap.models.cluster_nodes_metrocluster.ClusterNodesMetroclusterSchema", unknown=EXCLUDE, data_key="metrocluster")
    r""" The metrocluster field of the node_response_records. """

    model = fields.Str(data_key="model")
    r""" The model field of the node_response_records.

Example: FAS3070 """

    name = fields.Str(data_key="name")
    r""" The name field of the node_response_records.

Example: node-01 """

    nvram = fields.Nested("netapp_ontap.models.cluster_nodes_nvram.ClusterNodesNvramSchema", unknown=EXCLUDE, data_key="nvram")
    r""" The nvram field of the node_response_records. """

    owner = fields.Str(data_key="owner")
    r""" Owner of the node.

Example: Example Corp """

    serial_number = fields.Str(data_key="serial_number")
    r""" The serial_number field of the node_response_records.

Example: 4048820-60-9 """

    service_processor = fields.Nested("netapp_ontap.models.service_processor.ServiceProcessorSchema", unknown=EXCLUDE, data_key="service_processor")
    r""" The service_processor field of the node_response_records. """

    snaplock = fields.Nested("netapp_ontap.models.cluster_nodes_snaplock.ClusterNodesSnaplockSchema", unknown=EXCLUDE, data_key="snaplock")
    r""" The snaplock field of the node_response_records. """

    state = fields.Str(data_key="state")
    r""" State of the node:

* <i>up</i> - Node is up and operational.
* <i>booting</i> - Node is booting up.
* <i>down</i> - Node has stopped or is dumping core.
* <i>taken_over</i> - Node has been taken over by its HA partner and is not yet waiting for giveback.
* <i>waiting_for_giveback</i> - Node has been taken over by its HA partner and is waiting for the HA partner to giveback disks.
* <i>degraded</i> - Node has one or more critical services offline.
* <i>unknown</i> - Node or its HA partner cannot be contacted and there is no information on the node's state.


Valid choices:

* up
* booting
* down
* taken_over
* waiting_for_giveback
* degraded
* unknown """

    statistics = fields.Nested("netapp_ontap.models.node_statistics.NodeStatisticsSchema", unknown=EXCLUDE, data_key="statistics")
    r""" The statistics field of the node_response_records. """

    storage_configuration = fields.Str(data_key="storage_configuration")
    r""" The storage configuration in the system. Possible values:

* <i>mixed_path</i>
* <i>single_path</i>
* <i>multi_path</i>
* <i>quad_path</i>
* <i>mixed_path_ha</i>
* <i>single_path_ha</i>
* <i>multi_path_ha</i>
* <i>quad_path_ha</i>
* <i>unknown</i>


Valid choices:

* unknown
* single_path
* multi_path
* mixed_path
* quad_path
* single_path_ha
* multi_path_ha
* mixed_path_ha
* quad_path_ha """

    system_id = fields.Str(data_key="system_id")
    r""" The system_id field of the node_response_records.

Example: 92027651 """

    system_machine_type = fields.Str(data_key="system_machine_type")
    r""" OEM system machine type.

Example: 7Y56-CTOWW1 """

    uptime = Size(data_key="uptime")
    r""" The total time, in seconds, that the node has been up.

Example: 300536 """

    uuid = fields.Str(data_key="uuid")
    r""" The uuid field of the node_response_records.

Example: 4ea7a442-86d1-11e0-ae1c-123478563412 """

    vendor_serial_number = fields.Str(data_key="vendor_serial_number")
    r""" OEM vendor serial number.

Example: 791603000068 """

    version = fields.Nested("netapp_ontap.models.version.VersionSchema", unknown=EXCLUDE, data_key="version")
    r""" The version field of the node_response_records. """

    vm = fields.Nested("netapp_ontap.models.cluster_nodes_vm.ClusterNodesVmSchema", unknown=EXCLUDE, data_key="vm")
    r""" The vm field of the node_response_records. """

    @property
    def resource(self):
        return NodeResponseRecords

    gettable_fields = [
        "links",
        "cluster_interfaces.links",
        "cluster_interfaces.ip",
        "cluster_interfaces.name",
        "cluster_interfaces.uuid",
        "controller",
        "date",
        "external_cache",
        "ha",
        "hw_assist",
        "is_spares_low",
        "location",
        "management_interfaces.links",
        "management_interfaces.ip",
        "management_interfaces.name",
        "management_interfaces.uuid",
        "membership",
        "metric",
        "metrocluster",
        "model",
        "name",
        "nvram",
        "owner",
        "serial_number",
        "service_processor",
        "snaplock",
        "state",
        "statistics",
        "storage_configuration",
        "system_id",
        "system_machine_type",
        "uptime",
        "uuid",
        "vendor_serial_number",
        "version",
        "vm",
    ]
    """links,cluster_interfaces.links,cluster_interfaces.ip,cluster_interfaces.name,cluster_interfaces.uuid,controller,date,external_cache,ha,hw_assist,is_spares_low,location,management_interfaces.links,management_interfaces.ip,management_interfaces.name,management_interfaces.uuid,membership,metric,metrocluster,model,name,nvram,owner,serial_number,service_processor,snaplock,state,statistics,storage_configuration,system_id,system_machine_type,uptime,uuid,vendor_serial_number,version,vm,"""

    patchable_fields = [
        "controller",
        "ha",
        "location",
        "metrocluster",
        "name",
        "nvram",
        "owner",
        "service_processor",
        "vm",
    ]
    """controller,ha,location,metrocluster,name,nvram,owner,service_processor,vm,"""

    postable_fields = [
        "cluster_interface",
        "controller",
        "ha",
        "location",
        "management_interface",
        "metrocluster",
        "name",
        "nvram",
        "owner",
        "service_processor",
        "vm",
    ]
    """cluster_interface,controller,ha,location,management_interface,metrocluster,name,nvram,owner,service_processor,vm,"""


class NodeResponseRecords(Resource):

    _schema = NodeResponseRecordsSchema
