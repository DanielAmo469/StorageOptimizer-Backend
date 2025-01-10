r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SvmSnapmirror", "SvmSnapmirrorSchema"]
__pdoc__ = {
    "SvmSnapmirrorSchema.resource": False,
    "SvmSnapmirrorSchema.opts": False,
    "SvmSnapmirror": False,
}


class SvmSnapmirrorSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SvmSnapmirror object"""

    is_protected = fields.Boolean(data_key="is_protected")
    r""" Specifies whether the SVM is a SnapMirror source SVM, using SnapMirror to protect its data. """

    protected_volumes_count = Size(data_key="protected_volumes_count")
    r""" Specifies the number of SVM DR protected volumes in the SVM. """

    @property
    def resource(self):
        return SvmSnapmirror

    gettable_fields = [
        "is_protected",
        "protected_volumes_count",
    ]
    """is_protected,protected_volumes_count,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class SvmSnapmirror(Resource):

    _schema = SvmSnapmirrorSchema
