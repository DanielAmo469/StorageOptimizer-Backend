r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeEfficiency", "VolumeEfficiencySchema"]
__pdoc__ = {
    "VolumeEfficiencySchema.resource": False,
    "VolumeEfficiencySchema.opts": False,
    "VolumeEfficiency": False,
}


class VolumeEfficiencySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeEfficiency object"""

    application_io_size = fields.Str(data_key="application_io_size")
    r""" Block size to use by compression.

Valid choices:

* 8k
* auto """

    auto_state = fields.Str(data_key="auto_state")
    r""" Automatic deduplication schedule volume state.<br>auto &dash; Volumes with auto_state set to auto start post-process deduplication automatically.<br>deprioritized &dash; Volumes with auto_state set to deprioritized do not start post-process deduplication automatically.

Valid choices:

* auto
* deprioritized """

    compaction = fields.Str(data_key="compaction")
    r""" The system can be enabled/disabled compaction.<br>inline &dash; Data will be compacted first and written to the volume.<br>none &dash; None<br>mixed &dash; Read only field for FlexGroups, where some of the constituent volumes are compaction enabled and some are disabled.

Valid choices:

* inline
* none
* mixed """

    compression = fields.Str(data_key="compression")
    r""" The system can be enabled/disabled compression.<br>inline &dash; Data will be compressed first and written to the volume. <br>background &dash; Data will be written to the volume and compressed later. <br>both &dash; Inline compression compresses the data and write to the volume, background compression compresses only the blocks on which inline compression is not run.<br>none &dash; None<br>mixed &dash; Read only field for FlexGroups, where some of the constituent volumes are compression enabled and some are disabled. <br>Note that On volumes with container compression enabled, background compression refers to inactive data compression scan enabled on the volume.

Valid choices:

* inline
* background
* both
* none
* mixed """

    compression_type = fields.Str(data_key="compression_type")
    r""" Compression type to use by compression. Valid for PATCH and GET.

Valid choices:

* none
* secondary
* adaptive """

    cross_volume_dedupe = fields.Str(data_key="cross_volume_dedupe")
    r""" The system can be enabled/disabled cross volume dedupe. it can be enabled only when dedupe is enabled.<br>inline &dash; Data will be cross volume deduped first and written to the volume.<br>background &dash; Data will be written to the volume and cross volume deduped later.<br>both &dash; Inline cross volume dedupe dedupes the data and write to the volume, background cross volume dedupe dedupes only the blocks on which inline dedupe is not run.<br>none &dash; None<br>mixed &dash; Read only field for FlexGroups, where some of the constituent volumes are cross volume dedupe enabled and some are disabled.

Valid choices:

* inline
* background
* both
* none
* mixed """

    dedupe = fields.Str(data_key="dedupe")
    r""" The system can be enabled/disabled dedupe.<br>inline &dash; Data will be deduped first and written to the volume.<br>background &dash; Data will be written to the volume and deduped later.<br>both &dash; Inline dedupe dedupes the data and write to the volume, background dedupe dedupes only the blocks on which inline dedupe is not run.<br>none &dash; None<br>mixed &dash; Read only field for FlexGroups, where some of the constituent volumes are dedupe enabled and some are disabled.

Valid choices:

* inline
* background
* both
* none
* mixed """

    has_savings = fields.Boolean(data_key="has_savings")
    r""" When true, indicates that the volume contains shared(deduplication, file clones) or compressed data. """

    idcs_scanner = fields.Nested("netapp_ontap.models.volume_efficiency_idcs_scanner.VolumeEfficiencyIdcsScannerSchema", unknown=EXCLUDE, data_key="idcs_scanner")
    r""" The idcs_scanner field of the volume_efficiency. """

    last_op_begin = fields.Str(data_key="last_op_begin")
    r""" Last sis operation begin timestamp. """

    last_op_end = fields.Str(data_key="last_op_end")
    r""" Last sis operation end timestamp. """

    last_op_err = fields.Str(data_key="last_op_err")
    r""" Last sis operation error text. """

    last_op_size = Size(data_key="last_op_size")
    r""" Last sis operation size. """

    last_op_state = fields.Str(data_key="last_op_state")
    r""" Last sis operation state. """

    logging_enabled = fields.Boolean(data_key="logging_enabled")
    r""" When true, indicates that space savings for any newly-written data are being logged. """

    op_state = fields.Str(data_key="op_state")
    r""" Sis status of the volume.

Valid choices:

* idle
* initializing
* active
* undoing
* pending
* downgrading
* disabled """

    policy = fields.Nested("netapp_ontap.models.volume_efficiency_policy1.VolumeEfficiencyPolicy1Schema", unknown=EXCLUDE, data_key="policy")
    r""" The policy field of the volume_efficiency. """

    progress = fields.Str(data_key="progress")
    r""" Sis progress of the volume. """

    scanner = fields.Nested("netapp_ontap.models.volume_efficiency_scanner.VolumeEfficiencyScannerSchema", unknown=EXCLUDE, data_key="scanner")
    r""" The scanner field of the volume_efficiency. """

    schedule = fields.Str(data_key="schedule")
    r""" Schedule associated with volume. """

    space_savings = fields.Nested("netapp_ontap.models.volume_efficiency_space_savings.VolumeEfficiencySpaceSavingsSchema", unknown=EXCLUDE, data_key="space_savings")
    r""" The space_savings field of the volume_efficiency. """

    state = fields.Str(data_key="state")
    r""" Storage efficiency state of the volume. Currently, this field supports POST/PATCH only for RW (Read-Write) volumes on FSx for ONTAP and Cloud Volumes ONTAP.<br>disabled &dash; All storage efficiency features are disabled.<br>mixed &dash; Read-only field for FlexGroup volumes, storage efficiency is enabled on certain constituents and disabled on others.<br>On FSx for ONTAP and Cloud Volumes ONTAP &dash; <br> &emsp; enabled &dash; All supported storage efficiency features for the volume are enabled.<br> &emsp; custom &dash; Read-only field currently only supported for the FSx for ONTAP and Cloud Volumes ONTAP, user-defined storage efficiency features are enabled.<br>For other platforms &dash; <br> &emsp; enabled &dash; At least one storage efficiency feature for the volume is enabled.

Valid choices:

* disabled
* enabled
* mixed
* custom """

    storage_efficiency_mode = fields.Str(data_key="storage_efficiency_mode")
    r""" Storage efficiency mode used by volume. This parameter is supported only on AFF platform.

Valid choices:

* default
* efficient """

    type = fields.Str(data_key="type")
    r""" Sis Type of the volume.

Valid choices:

* regular
* snapvault """

    volume_path = fields.Str(data_key="volume_path")
    r""" Absolute volume path of the volume. """

    @property
    def resource(self):
        return VolumeEfficiency

    gettable_fields = [
        "application_io_size",
        "auto_state",
        "compaction",
        "compression",
        "compression_type",
        "cross_volume_dedupe",
        "dedupe",
        "has_savings",
        "idcs_scanner",
        "last_op_begin",
        "last_op_end",
        "last_op_err",
        "last_op_size",
        "last_op_state",
        "logging_enabled",
        "op_state",
        "policy",
        "progress",
        "scanner",
        "schedule",
        "space_savings",
        "state",
        "storage_efficiency_mode",
        "type",
        "volume_path",
    ]
    """application_io_size,auto_state,compaction,compression,compression_type,cross_volume_dedupe,dedupe,has_savings,idcs_scanner,last_op_begin,last_op_end,last_op_err,last_op_size,last_op_state,logging_enabled,op_state,policy,progress,scanner,schedule,space_savings,state,storage_efficiency_mode,type,volume_path,"""

    patchable_fields = [
        "application_io_size",
        "compaction",
        "compression",
        "compression_type",
        "cross_volume_dedupe",
        "dedupe",
        "idcs_scanner",
        "policy",
        "scanner",
        "space_savings",
        "state",
        "storage_efficiency_mode",
    ]
    """application_io_size,compaction,compression,compression_type,cross_volume_dedupe,dedupe,idcs_scanner,policy,scanner,space_savings,state,storage_efficiency_mode,"""

    postable_fields = [
        "application_io_size",
        "compaction",
        "compression",
        "compression_type",
        "cross_volume_dedupe",
        "dedupe",
        "idcs_scanner",
        "policy",
        "scanner",
        "space_savings",
        "state",
        "storage_efficiency_mode",
    ]
    """application_io_size,compaction,compression,compression_type,cross_volume_dedupe,dedupe,idcs_scanner,policy,scanner,space_savings,state,storage_efficiency_mode,"""


class VolumeEfficiency(Resource):

    _schema = VolumeEfficiencySchema
