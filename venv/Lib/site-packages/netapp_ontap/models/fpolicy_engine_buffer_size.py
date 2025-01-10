r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FpolicyEngineBufferSize", "FpolicyEngineBufferSizeSchema"]
__pdoc__ = {
    "FpolicyEngineBufferSizeSchema.resource": False,
    "FpolicyEngineBufferSizeSchema.opts": False,
    "FpolicyEngineBufferSize": False,
}


class FpolicyEngineBufferSizeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FpolicyEngineBufferSize object"""

    recv_buffer = Size(data_key="recv_buffer")
    r""" Specifies the receive buffer size of the connected socket for the FPolicy server. Default value is 256KB. """

    send_buffer = Size(data_key="send_buffer")
    r""" Specifies the send buffer size of the connected socket for the FPolicy server. Default value 1MB. """

    @property
    def resource(self):
        return FpolicyEngineBufferSize

    gettable_fields = [
        "recv_buffer",
        "send_buffer",
    ]
    """recv_buffer,send_buffer,"""

    patchable_fields = [
        "recv_buffer",
        "send_buffer",
    ]
    """recv_buffer,send_buffer,"""

    postable_fields = [
        "recv_buffer",
        "send_buffer",
    ]
    """recv_buffer,send_buffer,"""


class FpolicyEngineBufferSize(Resource):

    _schema = FpolicyEngineBufferSizeSchema
