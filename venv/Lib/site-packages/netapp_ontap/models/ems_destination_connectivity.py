r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsDestinationConnectivity", "EmsDestinationConnectivitySchema"]
__pdoc__ = {
    "EmsDestinationConnectivitySchema.resource": False,
    "EmsDestinationConnectivitySchema.opts": False,
    "EmsDestinationConnectivity": False,
}


class EmsDestinationConnectivitySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsDestinationConnectivity object"""

    errors = fields.List(fields.Nested("netapp_ontap.models.ems_connectivity_error.EmsConnectivityErrorSchema", unknown=EXCLUDE), data_key="errors")
    r""" A list of errors encountered during connectivity checks. """

    state = fields.Str(data_key="state")
    r""" Current connectivity state.

Valid choices:

* success
* fail
* not_supported """

    @property
    def resource(self):
        return EmsDestinationConnectivity

    gettable_fields = [
        "errors",
        "state",
    ]
    """errors,state,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class EmsDestinationConnectivity(Resource):

    _schema = EmsDestinationConnectivitySchema
