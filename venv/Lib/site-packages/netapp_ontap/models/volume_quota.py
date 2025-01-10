r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeQuota", "VolumeQuotaSchema"]
__pdoc__ = {
    "VolumeQuotaSchema.resource": False,
    "VolumeQuotaSchema.opts": False,
    "VolumeQuota": False,
}


class VolumeQuotaSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeQuota object"""

    enabled = fields.Boolean(data_key="enabled")
    r""" This option is used to enable or disable the quota for the volume. This option is valid only in PATCH. Quotas are enabled for FlexVols or FlexGroup volumes when the quota state is "on". Quotas are disabled for FlexVols or FlexGroup volumes when the quota state is "off". """

    state = fields.Str(data_key="state")
    r""" Quota state of the volume

Valid choices:

* corrupt
* initializing
* mixed
* off
* on
* resizing """

    @property
    def resource(self):
        return VolumeQuota

    gettable_fields = [
        "state",
    ]
    """state,"""

    patchable_fields = [
        "enabled",
    ]
    """enabled,"""

    postable_fields = [
    ]
    """"""


class VolumeQuota(Resource):

    _schema = VolumeQuotaSchema
