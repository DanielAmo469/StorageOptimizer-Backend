r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SplitLoadLoad", "SplitLoadLoadSchema"]
__pdoc__ = {
    "SplitLoadLoadSchema.resource": False,
    "SplitLoadLoadSchema.opts": False,
    "SplitLoadLoad": False,
}


class SplitLoadLoadSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SplitLoadLoad object"""

    allowable = Size(data_key="allowable")
    r""" Specifies the available file clone split load on the node. """

    current = Size(data_key="current")
    r""" Specifies the current on-going file clone split load on the node. """

    maximum = Size(data_key="maximum")
    r""" Specifies the maximum allowable file clone split load on the node at any point in time. """

    token_reserved = Size(data_key="token_reserved")
    r""" Specifies the file clone split load on the node reserved for tokens. """

    @property
    def resource(self):
        return SplitLoadLoad

    gettable_fields = [
        "allowable",
        "current",
        "maximum",
        "token_reserved",
    ]
    """allowable,current,maximum,token_reserved,"""

    patchable_fields = [
        "maximum",
    ]
    """maximum,"""

    postable_fields = [
    ]
    """"""


class SplitLoadLoad(Resource):

    _schema = SplitLoadLoadSchema
