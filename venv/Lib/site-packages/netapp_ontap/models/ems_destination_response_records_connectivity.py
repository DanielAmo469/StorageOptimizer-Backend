r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsDestinationResponseRecordsConnectivity", "EmsDestinationResponseRecordsConnectivitySchema"]
__pdoc__ = {
    "EmsDestinationResponseRecordsConnectivitySchema.resource": False,
    "EmsDestinationResponseRecordsConnectivitySchema.opts": False,
    "EmsDestinationResponseRecordsConnectivity": False,
}


class EmsDestinationResponseRecordsConnectivitySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsDestinationResponseRecordsConnectivity object"""

    errors = fields.List(fields.Nested("netapp_ontap.models.ems_destination_connectivity_errors.EmsDestinationConnectivityErrorsSchema", unknown=EXCLUDE), data_key="errors")
    r""" A list of errors encountered during connectivity checks. """

    state = fields.Str(data_key="state")
    r""" Current connectivity state.

Valid choices:

* success
* fail
* not_supported """

    @property
    def resource(self):
        return EmsDestinationResponseRecordsConnectivity

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


class EmsDestinationResponseRecordsConnectivity(Resource):

    _schema = EmsDestinationResponseRecordsConnectivitySchema
