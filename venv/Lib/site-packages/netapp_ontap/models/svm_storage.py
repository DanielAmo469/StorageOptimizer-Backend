r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SvmStorage", "SvmStorageSchema"]
__pdoc__ = {
    "SvmStorageSchema.resource": False,
    "SvmStorageSchema.opts": False,
    "SvmStorage": False,
}


class SvmStorageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SvmStorage object"""

    allocated = Size(data_key="allocated")
    r""" Total size of the volumes in SVM, in bytes. """

    available = Size(data_key="available")
    r""" Currently available storage capacity in SVM, in bytes. """

    limit = Size(data_key="limit")
    r""" Maximum storage permitted on a single SVM, in bytes. """

    limit_threshold_alert = Size(data_key="limit_threshold_alert")
    r""" Indicates at what percentage of storage capacity an alert message is sent. The default value is 90. """

    limit_threshold_exceeded = fields.Boolean(data_key="limit_threshold_exceeded")
    r""" Indicates whether the total storage capacity exceeds the alert percentage. """

    used_percentage = Size(data_key="used_percentage")
    r""" The percentage of storage capacity used. """

    @property
    def resource(self):
        return SvmStorage

    gettable_fields = [
        "allocated",
        "available",
        "limit",
        "limit_threshold_alert",
        "limit_threshold_exceeded",
        "used_percentage",
    ]
    """allocated,available,limit,limit_threshold_alert,limit_threshold_exceeded,used_percentage,"""

    patchable_fields = [
        "limit",
        "limit_threshold_alert",
    ]
    """limit,limit_threshold_alert,"""

    postable_fields = [
        "limit",
        "limit_threshold_alert",
    ]
    """limit,limit_threshold_alert,"""


class SvmStorage(Resource):

    _schema = SvmStorageSchema
