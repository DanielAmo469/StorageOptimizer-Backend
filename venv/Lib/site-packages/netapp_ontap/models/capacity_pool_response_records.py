r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CapacityPoolResponseRecords", "CapacityPoolResponseRecordsSchema"]
__pdoc__ = {
    "CapacityPoolResponseRecordsSchema.resource": False,
    "CapacityPoolResponseRecordsSchema.opts": False,
    "CapacityPoolResponseRecords": False,
}


class CapacityPoolResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CapacityPoolResponseRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the capacity_pool_response_records. """

    license_manager = fields.Nested("netapp_ontap.resources.license_manager.LicenseManagerSchema", unknown=EXCLUDE, data_key="license_manager")
    r""" The license_manager field of the capacity_pool_response_records. """

    nodes = fields.List(fields.Nested("netapp_ontap.models.capacity_pool_nodes.CapacityPoolNodesSchema", unknown=EXCLUDE), data_key="nodes")
    r""" Nodes in the cluster associated with this capacity pool. """

    serial_number = fields.Str(data_key="serial_number")
    r""" Serial number of the capacity pool license.

Example: 390000100 """

    @property
    def resource(self):
        return CapacityPoolResponseRecords

    gettable_fields = [
        "links",
        "license_manager.links",
        "license_manager.uuid",
        "nodes",
        "serial_number",
    ]
    """links,license_manager.links,license_manager.uuid,nodes,serial_number,"""

    patchable_fields = [
        "license_manager.links",
        "license_manager.uuid",
        "nodes",
        "serial_number",
    ]
    """license_manager.links,license_manager.uuid,nodes,serial_number,"""

    postable_fields = [
        "license_manager.links",
        "license_manager.uuid",
        "nodes",
        "serial_number",
    ]
    """license_manager.links,license_manager.uuid,nodes,serial_number,"""


class CapacityPoolResponseRecords(Resource):

    _schema = CapacityPoolResponseRecordsSchema
