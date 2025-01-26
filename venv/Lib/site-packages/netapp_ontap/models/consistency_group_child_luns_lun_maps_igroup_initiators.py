r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupChildLunsLunMapsIgroupInitiators", "ConsistencyGroupChildLunsLunMapsIgroupInitiatorsSchema"]
__pdoc__ = {
    "ConsistencyGroupChildLunsLunMapsIgroupInitiatorsSchema.resource": False,
    "ConsistencyGroupChildLunsLunMapsIgroupInitiatorsSchema.opts": False,
    "ConsistencyGroupChildLunsLunMapsIgroupInitiators": False,
}


class ConsistencyGroupChildLunsLunMapsIgroupInitiatorsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupChildLunsLunMapsIgroupInitiators object"""

    comment = fields.Str(data_key="comment")
    r""" A comment available for use by the administrator.


Example: my comment """

    name = fields.Str(data_key="name")
    r""" Name of initiator that is a member of the initiator group.


Example: iqn.1998-01.com.corp.iscsi:name1 """

    @property
    def resource(self):
        return ConsistencyGroupChildLunsLunMapsIgroupInitiators

    gettable_fields = [
        "comment",
        "name",
    ]
    """comment,name,"""

    patchable_fields = [
        "comment",
        "name",
    ]
    """comment,name,"""

    postable_fields = [
        "comment",
        "name",
    ]
    """comment,name,"""


class ConsistencyGroupChildLunsLunMapsIgroupInitiators(Resource):

    _schema = ConsistencyGroupChildLunsLunMapsIgroupInitiatorsSchema
