r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ShelfFrusPsu", "ShelfFrusPsuSchema"]
__pdoc__ = {
    "ShelfFrusPsuSchema.resource": False,
    "ShelfFrusPsuSchema.opts": False,
    "ShelfFrusPsu": False,
}


class ShelfFrusPsuSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShelfFrusPsu object"""

    crest_factor = Size(data_key="crest_factor")
    r""" The ratio of the peak voltage to the root-mean-square voltage

Example: 92 """

    model = fields.Str(data_key="model")
    r""" The model field of the shelf_frus_psu.

Example: 00 """

    power_drawn = Size(data_key="power_drawn")
    r""" Power drawn, in watts

Example: 210 """

    power_rating = Size(data_key="power_rating")
    r""" Power rating, in watts

Example: 1600 """

    @property
    def resource(self):
        return ShelfFrusPsu

    gettable_fields = [
        "crest_factor",
        "model",
        "power_drawn",
        "power_rating",
    ]
    """crest_factor,model,power_drawn,power_rating,"""

    patchable_fields = [
        "crest_factor",
        "model",
        "power_drawn",
        "power_rating",
    ]
    """crest_factor,model,power_drawn,power_rating,"""

    postable_fields = [
        "crest_factor",
        "model",
        "power_drawn",
        "power_rating",
    ]
    """crest_factor,model,power_drawn,power_rating,"""


class ShelfFrusPsu(Resource):

    _schema = ShelfFrusPsuSchema
