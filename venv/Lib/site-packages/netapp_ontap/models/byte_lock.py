r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ByteLock", "ByteLockSchema"]
__pdoc__ = {
    "ByteLockSchema.resource": False,
    "ByteLockSchema.opts": False,
    "ByteLock": False,
}


class ByteLockSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ByteLock object"""

    exclusive = fields.Boolean(data_key="exclusive")
    r""" Indicates whether it is an exclusive bytelock. """

    length = Size(data_key="length")
    r""" Length of the bytelock starting from the offset.

Example: 10 """

    mandatory = fields.Boolean(data_key="mandatory")
    r""" Indicates whether or not the bytelock is mandatory. """

    offset = Size(data_key="offset")
    r""" Starting offset for a bytelock.

Example: 100 """

    soft = fields.Boolean(data_key="soft")
    r""" Indicates whether it is a soft bytelock. """

    super = fields.Boolean(data_key="super")
    r""" Indicates whether it is a super bytelock. """

    @property
    def resource(self):
        return ByteLock

    gettable_fields = [
        "exclusive",
        "length",
        "mandatory",
        "offset",
        "soft",
        "super",
    ]
    """exclusive,length,mandatory,offset,soft,super,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ByteLock(Resource):

    _schema = ByteLockSchema
