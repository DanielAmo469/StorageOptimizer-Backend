r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MetroclusterDiagnosticsConnection", "MetroclusterDiagnosticsConnectionSchema"]
__pdoc__ = {
    "MetroclusterDiagnosticsConnectionSchema.resource": False,
    "MetroclusterDiagnosticsConnectionSchema.opts": False,
    "MetroclusterDiagnosticsConnection": False,
}


class MetroclusterDiagnosticsConnectionSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MetroclusterDiagnosticsConnection object"""

    details = fields.List(fields.Nested("netapp_ontap.models.metrocluster_diag_connection_details.MetroclusterDiagConnectionDetailsSchema", unknown=EXCLUDE), data_key="details")
    r""" Display details of the MetroCluster check for connections. """

    state = fields.Str(data_key="state")
    r""" Status of diagnostic operation for this component.

Valid choices:

* ok
* warning
* not_run
* not_applicable """

    summary = fields.Nested("netapp_ontap.models.error_arguments.ErrorArgumentsSchema", unknown=EXCLUDE, data_key="summary")
    r""" The summary field of the metrocluster_diagnostics_connection. """

    timestamp = ImpreciseDateTime(data_key="timestamp")
    r""" Time of the most recent diagnostic operation for this component

Example: 2016-03-10T22:35:16.000+0000 """

    @property
    def resource(self):
        return MetroclusterDiagnosticsConnection

    gettable_fields = [
        "details",
        "state",
        "summary",
        "timestamp",
    ]
    """details,state,summary,timestamp,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class MetroclusterDiagnosticsConnection(Resource):

    _schema = MetroclusterDiagnosticsConnectionSchema
