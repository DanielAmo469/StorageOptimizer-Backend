r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LunQosPolicy", "LunQosPolicySchema"]
__pdoc__ = {
    "LunQosPolicySchema.resource": False,
    "LunQosPolicySchema.opts": False,
    "LunQosPolicy": False,
}


class LunQosPolicySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LunQosPolicy object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the lun_qos_policy. """

    name = fields.Str(data_key="name")
    r""" The name of the QoS policy. To remove the QoS policy from a LUN, leaving it with no QoS policy, set this property to an empty string ("") in a PATCH request. Valid in POST and PATCH.


Example: qos1 """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the QoS policy. Valid in POST and PATCH.


Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return LunQosPolicy

    gettable_fields = [
        "links",
        "name",
        "uuid",
    ]
    """links,name,uuid,"""

    patchable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class LunQosPolicy(Resource):

    _schema = LunQosPolicySchema
