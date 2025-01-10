r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NasApplicationComponentsQosPolicy", "NasApplicationComponentsQosPolicySchema"]
__pdoc__ = {
    "NasApplicationComponentsQosPolicySchema.resource": False,
    "NasApplicationComponentsQosPolicySchema.opts": False,
    "NasApplicationComponentsQosPolicy": False,
}


class NasApplicationComponentsQosPolicySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NasApplicationComponentsQosPolicy object"""

    name = fields.Str(data_key="name")
    r""" The name of an existing QoS policy. """

    uuid = fields.Str(data_key="uuid")
    r""" The UUID of an existing QoS policy. Usage: &lt;UUID&gt; """

    @property
    def resource(self):
        return NasApplicationComponentsQosPolicy

    gettable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class NasApplicationComponentsQosPolicy(Resource):

    _schema = NasApplicationComponentsQosPolicySchema
