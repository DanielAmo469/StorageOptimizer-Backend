r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FcInterfaceRecommendMessage", "FcInterfaceRecommendMessageSchema"]
__pdoc__ = {
    "FcInterfaceRecommendMessageSchema.resource": False,
    "FcInterfaceRecommendMessageSchema.opts": False,
    "FcInterfaceRecommendMessage": False,
}


class FcInterfaceRecommendMessageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FcInterfaceRecommendMessage object"""

    arguments = fields.List(fields.Nested("netapp_ontap.models.error_arguments.ErrorArgumentsSchema", unknown=EXCLUDE), data_key="arguments")
    r""" The message substitution arguments. """

    code = fields.Str(data_key="code")
    r""" The message code. Possible messages:
  ONTAP Error Response Codes
  | Error Code | Description |
  | ---------- | ----------- |
  | 5375959 | Network ports are disabled. |
  | 5375960 | Network ports are enabled, but not reporting a connected FC fabric. |
  | 5375961 | The limit for the number of FC network interfaces on a cluster node has been reached. |
  | 5375962 | The limit for the number of FC network interfaces on a port has been reached. |
  | 5375963 | An HA pair of cluster nodes has a discrepancy in the presence of FC ports. |
  | 5375964 | An HA pair of cluster nodes has a discrepancy in support for an FC data protocol. |
  | 5375965 | An HA pair of cluster nodes cannot be reached from the same FC fabrics. |
  | 5375966 | A cluster node cannot be reached from all of the FC fabrics from which other cluster nodes with FC interfaces in the SVM can be reached. |
  | 5375967 | The limit for the number of FC network interfaces on a cluster node has been exceeded. |
  | 5375968 | The limit for the number of FC network interfaces on an FC port has been exceeded. |
  | 5375969 | The requested number of network interfaces per FC fabric per cluster node has not been achieved. |
  | 5375970 | The SVM cannot be reached from all of the FC fabrics to which the cluster is connected. |
  | 5375971 | The limit for the number of NVMe network interfaces on a cluster node has been exceeded. |
  | 5375972 | The limit for the number of cluster nodes containing NVMe network interfaces for the SVM has been exceeded. |
  | 5375973 | The SVM can be reached from a number of FC fabrics other than what is preferred. |


Example: 5375959 """

    message = fields.Str(data_key="message")
    r""" The message text.

Example: Network ports are disabled. """

    severity = fields.Str(data_key="severity")
    r""" The severity of the message. Message severities are as follows:
- `error` - Messages reporting problems that must be corrected before creating the FC network interfaces.
- `warning` - Messages indicating issues that need rectifying in order to achieve an optimal configuration.
- `informational` - Messages providing relevant information for consideration.


Valid choices:

* error
* warning
* informational """

    @property
    def resource(self):
        return FcInterfaceRecommendMessage

    gettable_fields = [
        "arguments",
        "code",
        "message",
        "severity",
    ]
    """arguments,code,message,severity,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class FcInterfaceRecommendMessage(Resource):

    _schema = FcInterfaceRecommendMessageSchema
