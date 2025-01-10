r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageSwitchPortsSfp", "StorageSwitchPortsSfpSchema"]
__pdoc__ = {
    "StorageSwitchPortsSfpSchema.resource": False,
    "StorageSwitchPortsSfpSchema.opts": False,
    "StorageSwitchPortsSfp": False,
}


class StorageSwitchPortsSfpSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageSwitchPortsSfp object"""

    serial_number = fields.Str(data_key="serial_number")
    r""" Storage switch port SFP serial number """

    transmitter_type = fields.Str(data_key="transmitter_type")
    r""" Storage switch port SFP transmitter type

Valid choices:

* unknown
* long_wave_laser
* short_wave_laser
* long_wave_laser_cost_reduced
* electrical
* ten_gig_base_sr
* ten_gig_base_lr
* ten_gig_base_er
* ten_gig_base_lx4
* ten_gig_base_sw
* ten_gig_base_lw
* ten_gig_base_ew """

    type = fields.Str(data_key="type")
    r""" Storage switch port SFP type

Valid choices:

* unknown
* other
* gbic
* embedded
* glm
* gbic_with_serial_id
* gbic_without_serial_id
* sfp_with_serial_id
* sfp_without_serial_id
* xfp
* x2_short
* x2_medium
* x2_tall
* xpak_short
* xpak_medium
* xpak_tall
* xenpak
* sfp_dw_dm
* qsfp
* x2_dw_dm
* gbic_not_installed
* small_form_factor """

    @property
    def resource(self):
        return StorageSwitchPortsSfp

    gettable_fields = [
        "serial_number",
        "transmitter_type",
        "type",
    ]
    """serial_number,transmitter_type,type,"""

    patchable_fields = [
        "serial_number",
        "transmitter_type",
        "type",
    ]
    """serial_number,transmitter_type,type,"""

    postable_fields = [
        "serial_number",
        "transmitter_type",
        "type",
    ]
    """serial_number,transmitter_type,type,"""


class StorageSwitchPortsSfp(Resource):

    _schema = StorageSwitchPortsSfpSchema
