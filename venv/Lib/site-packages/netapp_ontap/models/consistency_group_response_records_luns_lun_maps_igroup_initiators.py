r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupResponseRecordsLunsLunMapsIgroupInitiators", "ConsistencyGroupResponseRecordsLunsLunMapsIgroupInitiatorsSchema"]
__pdoc__ = {
    "ConsistencyGroupResponseRecordsLunsLunMapsIgroupInitiatorsSchema.resource": False,
    "ConsistencyGroupResponseRecordsLunsLunMapsIgroupInitiatorsSchema.opts": False,
    "ConsistencyGroupResponseRecordsLunsLunMapsIgroupInitiators": False,
}


class ConsistencyGroupResponseRecordsLunsLunMapsIgroupInitiatorsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupResponseRecordsLunsLunMapsIgroupInitiators object"""

    comment = fields.Str(data_key="comment")
    r""" A comment available for use by the administrator.


Example: my comment """

    name = fields.Str(data_key="name")
    r""" Name of initiator that is a member of the initiator group.


Example: iqn.1998-01.com.corp.iscsi:name1 """

    @property
    def resource(self):
        return ConsistencyGroupResponseRecordsLunsLunMapsIgroupInitiators

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


class ConsistencyGroupResponseRecordsLunsLunMapsIgroupInitiators(Resource):

    _schema = ConsistencyGroupResponseRecordsLunsLunMapsIgroupInitiatorsSchema
