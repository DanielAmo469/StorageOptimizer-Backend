r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FcpServiceTarget", "FcpServiceTargetSchema"]
__pdoc__ = {
    "FcpServiceTargetSchema.resource": False,
    "FcpServiceTargetSchema.opts": False,
    "FcpServiceTarget": False,
}


class FcpServiceTargetSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FcpServiceTarget object"""

    name = fields.Str(data_key="name")
    r""" The target name of the FC Protocol service. This is generated for the SVM during POST.<br/>
The FC Protocol target name is a world wide node name (WWNN).<br/>
If required, the target name can be modified using the ONTAP command line.


Example: 20:00:00:50:56:bb:b2:4b """

    @property
    def resource(self):
        return FcpServiceTarget

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class FcpServiceTarget(Resource):

    _schema = FcpServiceTargetSchema
