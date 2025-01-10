r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MetroclusterInterconnectMirror", "MetroclusterInterconnectMirrorSchema"]
__pdoc__ = {
    "MetroclusterInterconnectMirrorSchema.resource": False,
    "MetroclusterInterconnectMirrorSchema.opts": False,
    "MetroclusterInterconnectMirror": False,
}


class MetroclusterInterconnectMirrorSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MetroclusterInterconnectMirror object"""

    enabled = fields.Boolean(data_key="enabled")
    r""" Specifies the administrative state of the NVRAM mirror between partner nodes. """

    state = fields.Str(data_key="state")
    r""" Specifies the operational state of the NVRAM mirror between partner nodes.

Valid choices:

* online
* offline
* unknown """

    @property
    def resource(self):
        return MetroclusterInterconnectMirror

    gettable_fields = [
        "enabled",
        "state",
    ]
    """enabled,state,"""

    patchable_fields = [
        "enabled",
    ]
    """enabled,"""

    postable_fields = [
        "enabled",
    ]
    """enabled,"""


class MetroclusterInterconnectMirror(Resource):

    _schema = MetroclusterInterconnectMirrorSchema
