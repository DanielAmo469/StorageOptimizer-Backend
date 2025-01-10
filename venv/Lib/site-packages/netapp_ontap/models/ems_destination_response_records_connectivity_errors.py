r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsDestinationResponseRecordsConnectivityErrors", "EmsDestinationResponseRecordsConnectivityErrorsSchema"]
__pdoc__ = {
    "EmsDestinationResponseRecordsConnectivityErrorsSchema.resource": False,
    "EmsDestinationResponseRecordsConnectivityErrorsSchema.opts": False,
    "EmsDestinationResponseRecordsConnectivityErrors": False,
}


class EmsDestinationResponseRecordsConnectivityErrorsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsDestinationResponseRecordsConnectivityErrors object"""

    message = fields.Nested("netapp_ontap.models.ems_ui_message.EmsUiMessageSchema", unknown=EXCLUDE, data_key="message")
    r""" The message field of the ems_destination_response_records_connectivity_errors. """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the ems_destination_response_records_connectivity_errors. """

    @property
    def resource(self):
        return EmsDestinationResponseRecordsConnectivityErrors

    gettable_fields = [
        "message",
        "node.links",
        "node.name",
        "node.uuid",
    ]
    """message,node.links,node.name,node.uuid,"""

    patchable_fields = [
        "node.name",
        "node.uuid",
    ]
    """node.name,node.uuid,"""

    postable_fields = [
        "node.name",
        "node.uuid",
    ]
    """node.name,node.uuid,"""


class EmsDestinationResponseRecordsConnectivityErrors(Resource):

    _schema = EmsDestinationResponseRecordsConnectivityErrorsSchema
