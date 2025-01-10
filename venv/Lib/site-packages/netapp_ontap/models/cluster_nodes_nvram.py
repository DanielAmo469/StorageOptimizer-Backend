r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesNvram", "ClusterNodesNvramSchema"]
__pdoc__ = {
    "ClusterNodesNvramSchema.resource": False,
    "ClusterNodesNvramSchema.opts": False,
    "ClusterNodesNvram": False,
}


class ClusterNodesNvramSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesNvram object"""

    battery_state = fields.Str(data_key="battery_state")
    r""" Specifies status of the NVRAM battery. Possible values:

* <i>battery_ok</i>
* <i>battery_partially_discharged</i>
* <i>battery_fully_discharged</i>
* <i>battery_not_present</i>
* <i>battery_near_end_of_life</i>
* <i>battery_at_end_of_life</i>
* <i>battery_unknown</i>
* <i>battery_over_charged</i>
* <i>battery_fully_charged</i>


Valid choices:

* battery_ok
* battery_partially_discharged
* battery_fully_discharged
* battery_not_present
* battery_near_end_of_life
* battery_at_end_of_life
* battery_unknown
* battery_over_charged
* battery_fully_charged """

    id = Size(data_key="id")
    r""" Vendor specific NVRAM ID of the node. """

    @property
    def resource(self):
        return ClusterNodesNvram

    gettable_fields = [
        "battery_state",
        "id",
    ]
    """battery_state,id,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterNodesNvram(Resource):

    _schema = ClusterNodesNvramSchema
