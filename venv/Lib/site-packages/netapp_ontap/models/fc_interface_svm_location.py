r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FcInterfaceSvmLocation", "FcInterfaceSvmLocationSchema"]
__pdoc__ = {
    "FcInterfaceSvmLocationSchema.resource": False,
    "FcInterfaceSvmLocationSchema.opts": False,
    "FcInterfaceSvmLocation": False,
}


class FcInterfaceSvmLocationSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FcInterfaceSvmLocation object"""

    port = fields.Nested("netapp_ontap.resources.fc_port.FcPortSchema", unknown=EXCLUDE, data_key="port")
    r""" The port field of the fc_interface_svm_location. """

    @property
    def resource(self):
        return FcInterfaceSvmLocation

    gettable_fields = [
        "port.links",
        "port.name",
        "port.node",
        "port.uuid",
    ]
    """port.links,port.name,port.node,port.uuid,"""

    patchable_fields = [
        "port.name",
        "port.node",
        "port.uuid",
    ]
    """port.name,port.node,port.uuid,"""

    postable_fields = [
        "port.name",
        "port.node",
        "port.uuid",
    ]
    """port.name,port.node,port.uuid,"""


class FcInterfaceSvmLocation(Resource):

    _schema = FcInterfaceSvmLocationSchema
