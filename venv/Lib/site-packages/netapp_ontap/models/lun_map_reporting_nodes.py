r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LunMapReportingNodes", "LunMapReportingNodesSchema"]
__pdoc__ = {
    "LunMapReportingNodesSchema.resource": False,
    "LunMapReportingNodesSchema.opts": False,
    "LunMapReportingNodes": False,
}


class LunMapReportingNodesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LunMapReportingNodes object"""

    links = fields.Nested("netapp_ontap.models.lun_map_reporting_nodes_links.LunMapReportingNodesLinksSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the lun_map_reporting_nodes. """

    name = fields.Str(data_key="name")
    r""" The name of the node.<br/>
Either `uuid` or `name` are required in POST.


Example: node1 """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the node.<br/>
Either `uuid` or `name` are required in POST.


Example: 5ac8eb9c-4e32-dbaa-57ca-fb905976f54e """

    @property
    def resource(self):
        return LunMapReportingNodes

    gettable_fields = [
        "links",
        "name",
        "uuid",
    ]
    """links,name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class LunMapReportingNodes(Resource):

    _schema = LunMapReportingNodesSchema
