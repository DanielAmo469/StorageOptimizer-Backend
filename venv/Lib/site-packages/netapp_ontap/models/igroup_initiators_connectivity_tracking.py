r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["IgroupInitiatorsConnectivityTracking", "IgroupInitiatorsConnectivityTrackingSchema"]
__pdoc__ = {
    "IgroupInitiatorsConnectivityTrackingSchema.resource": False,
    "IgroupInitiatorsConnectivityTrackingSchema.opts": False,
    "IgroupInitiatorsConnectivityTracking": False,
}


class IgroupInitiatorsConnectivityTrackingSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the IgroupInitiatorsConnectivityTracking object"""

    connection_state = fields.Str(data_key="connection_state")
    r""" Connection state.

Valid choices:

* full
* none
* partial
* no_lun_maps """

    @property
    def resource(self):
        return IgroupInitiatorsConnectivityTracking

    gettable_fields = [
        "connection_state",
    ]
    """connection_state,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class IgroupInitiatorsConnectivityTracking(Resource):

    _schema = IgroupInitiatorsConnectivityTrackingSchema
