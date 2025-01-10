r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
This API manages asynchronous extended data protection (XDP) relationships for FlexVol volumes, FlexGroup volumes, or SVMs. It is also used to manage a synchronous relationship between FlexVol volumes, which provides zero RPO data protection and active synchronous relationship with automated failover between Application Consistency Group endpoints which provides zero RTO data protection.<br/>
To create an asynchronous extended data protection relationship with FlexVol volumes, FlexGroup volumes, Application Consistency Groups or SVMs, use the policy of type "async".<br/>
To create a synchronous relationship between FlexVol volumes, use the policy of type "sync" with sync_type of either "sync" or "strict_sync". To create an active synchronous relationship with automated failover between Application Consistency Group endpoints, use the policy of type "sync" with sync_type "automated_failover". You can create an asynchronous extended data protection relationship between the source and destination which can be used by the transfer APIs to perform SnapMirror "restore" operations.<br/>
To create FlexVol volume or FlexGroup volume SnapMirror relationships, the source volume must be in the "online" state and be a read_write type; the destination volume must be in the "online" state and be a data protection type.<br/>
In the case of an asynchronous or synchronous SnapMirror relationship for an Application Consistency Group of FlexVol volumes, SnapMirror creation results in the creation of an Application Consistency Group on the source cluster if it did not already exist with the exact same name and set of FlexVol volumes specified in the current operation. Additionally, if the specified Application Consistency Group is already present and is already a part of an existing SnapMirror relationship, the process fails. Creating an Application Consistency Group on the destination cluster is part of the SnapMirror creation workflow.<br/>
To create SnapMirror relationships between SVMs, the source SVM must be of subtype "default" and the destination SVM of subtype "dp_destination". Additionally, SVMs must be peered before a relationship can be established between them when the "create_destination" property is not specified. When the "create_destination" property is specified, the destination SVM is provisioned on the destination cluster and the SVM peer relationship is established between the source SVM and the new destination SVM, provided that the source SVM has SVM peering permissions for the destination cluster.<br/>
Data protection FlexVol volume SnapMirror relationships cannot be created using this API but existing relationships can be listed or managed.<br/>
The SnapMirror functionality is subdivided into relationship APIs and transfer APIs:
- SnapMirror relationship APIs are used to create and manage the SnapMirror relationships.
- SnapMirror transfer APIs are used to manage data transfers."""

import asyncio
from datetime import datetime
import inspect
from typing import Callable, Iterable, List, Optional, Union

try:
    RECLINE_INSTALLED = False
    import recline
    from recline.arg_types.choices import Choices
    from recline.commands import ReclineCommandError
    from netapp_ontap.resource_table import ResourceTable
    RECLINE_INSTALLED = True
except ImportError:
    pass

from marshmallow import fields, EXCLUDE  # type: ignore

import netapp_ontap
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size
from netapp_ontap import NetAppResponse, HostConnection
from netapp_ontap.validations import enum_validation, len_validation, integer_validation
from netapp_ontap.error import NetAppRestError


__all__ = ["SnapmirrorRelationship", "SnapmirrorRelationshipSchema"]
__pdoc__ = {
    "SnapmirrorRelationshipSchema.resource": False,
    "SnapmirrorRelationshipSchema.opts": False,
    "SnapmirrorRelationship.snapmirror_relationship_show": False,
    "SnapmirrorRelationship.snapmirror_relationship_create": False,
    "SnapmirrorRelationship.snapmirror_relationship_modify": False,
    "SnapmirrorRelationship.snapmirror_relationship_delete": False,
}


class SnapmirrorRelationshipSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SnapmirrorRelationship object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the snapmirror_relationship."""

    consistency_group_failover = fields.Nested("netapp_ontap.models.snapmirror_consistency_group_failover.SnapmirrorConsistencyGroupFailoverSchema", data_key="consistency_group_failover", unknown=EXCLUDE)
    r""" The consistency_group_failover field of the snapmirror_relationship."""

    create_destination = fields.Nested("netapp_ontap.models.snapmirror_destination_creation.SnapmirrorDestinationCreationSchema", data_key="create_destination", unknown=EXCLUDE)
    r""" The create_destination field of the snapmirror_relationship."""

    destination = fields.Nested("netapp_ontap.models.snapmirror_endpoint.SnapmirrorEndpointSchema", data_key="destination", unknown=EXCLUDE)
    r""" This property is the destination endpoint of the relationship. The destination endpoint can be a FlexVol volume, FlexGroup volume, Consistency Group, or SVM. For the POST request, the destination endpoint must be of type "DP" when the endpoint is a FlexVol volume or a FlexGroup volume. When specifying a Consistency Group as the destination endpoint, the "destination.consistency_group_volumes" property must be specified with the FlexVol volumes of type "DP". The POST request for SVM must have a destination endpoint of type "dp-destination". The destination endpoint path name must be specified in the "destination.path" property. For relationships of type "async", the destination endpoint for FlexVol volume and FlexGroup volume will change to type "RW" when the relationship status is "broken_off" and will revert to type "DP" when the relationship status is "snapmirrored" or "in_sync" using the PATCH request. The destination endpoint for SVM will change from "dp-destination" to type "default" when the relationship status is "broken_off" and will revert to type "dp-destination" when the relationship status is "snapmirrored" using the PATCH request. When the destination endpoint is a Consistency Group, the Consistency Group FlexVol volumes will change to type "RW" when the relationship status is "broken_off" and will revert to type "DP" when the relationship status is "in_sync" using the PATCH request."""

    exported_snapshot = fields.Str(
        data_key="exported_snapshot",
    )
    r""" Snapshot copy exported to clients on destination."""

    group_type = fields.Str(
        data_key="group_type",
        validate=enum_validation(['none', 'svm_dr', 'consistency_group', 'flexgroup']),
    )
    r""" Specifies the group type of the top level SnapMirror relationship. The volume relationships are shown as _none_, the SVMDR relationships are shown as _svm_dr_, the Consistency Group relationships are shown as _consistency_group_, and the FlexGroup volume relationships are shown as _flexgroup_.

Valid choices:

* none
* svm_dr
* consistency_group
* flexgroup"""

    healthy = fields.Boolean(
        data_key="healthy",
    )
    r""" Is the relationship healthy?"""

    identity_preservation = fields.Str(
        data_key="identity_preservation",
        validate=enum_validation(['full', 'exclude_network_config', 'exclude_network_and_protocol_config']),
    )
    r""" Specifies which configuration of the source SVM is replicated to the destination SVM. This property is applicable only for SVM data protection with "async" policy type. This "identity_preservation" overrides the "identity_preservation" set on the SnapMirror relationship's policy.

Valid choices:

* full
* exclude_network_config
* exclude_network_and_protocol_config"""

    lag_time = fields.Str(
        data_key="lag_time",
    )
    r""" Time since the exported Snapshot copy was created.

Example: PT8H35M42S"""

    last_transfer_network_compression_ratio = fields.Str(
        data_key="last_transfer_network_compression_ratio",
    )
    r""" Specifies the compression ratio achieved for the data sent over the wire with network compression enabled for the last successful transfer.

Example: 61"""

    last_transfer_type = fields.Str(
        data_key="last_transfer_type",
        validate=enum_validation(['initialize', 'update', 'resync', 'restore']),
    )
    r""" Specifies the operation type of the last transfer that occurred on the relationship. The _initialize_ transfer occurs when the relationship state changes from uninitialized to snapmirrored or in_sync. The _update_ transfer occurs when the snapshots are transferred from the source endpoint to the destination endpoint as part of scheduled or manual update. The _resync_ transfer occurs when the relationship state changes from broken_off to snapmirrored or in_sync. The _restore_ transfer occurs when the snapshot is restored from a destination endpoint to another endpoint.

Valid choices:

* initialize
* update
* resync
* restore"""

    policy = fields.Nested("netapp_ontap.models.snapmirror_relationship_policy.SnapmirrorRelationshipPolicySchema", data_key="policy", unknown=EXCLUDE)
    r""" The policy field of the snapmirror_relationship."""

    preserve = fields.Boolean(
        data_key="preserve",
    )
    r""" Set to true on resync to preserve Snapshot copies on the destination that are newer than the latest common Snapshot copy. This property is applicable only for relationships with FlexVol volume or FlexGroup volume endpoints and when the PATCH state is being changed to "snapmirrored"."""

    quick_resync = fields.Boolean(
        data_key="quick_resync",
    )
    r""" Set to true to reduce resync time by not preserving storage efficiency. This property is applicable only for relationships with FlexVol volume endpoints and SVMDR relationships when the PATCH state is being changed to "snapmirrored"."""

    recover_after_break = fields.Boolean(
        data_key="recover_after_break",
    )
    r""" Set to true to recover from a failed SnapMirror break operation on a FlexGroup volume relationship. This restores all destination FlexGroup constituent volumes to the latest Snapshot copy, and any writes to the read-write constituents are lost. This property is applicable only for SnapMirror relationships with FlexGroup volume endpoints and when the PATCH state is being changed to "broken_off"."""

    restore = fields.Boolean(
        data_key="restore",
    )
    r""" Set to true to create a relationship for restore. To trigger restore-transfer, use transfers POST on the restore relationship. SnapMirror relationships with the policy type "async" can be restored. SnapMirror relationships with the policy type "sync" cannot be restored."""

    restore_to_snapshot = fields.Str(
        data_key="restore_to_snapshot",
    )
    r""" Specifies the Snapshot copy to restore to on the destination during the break operation. This property is applicable only for SnapMirror relationships with FlexVol volume endpoints and when the PATCH state is being changed to "broken_off"."""

    source = fields.Nested("netapp_ontap.models.snapmirror_endpoint.SnapmirrorEndpointSchema", data_key="source", unknown=EXCLUDE)
    r""" This property is the source endpoint of the relationship. The source endpoint can be a FlexVol volume, FlexGroup volume, Consistency Group, or SVM. To establish a SnapMirror relationship with SVM as source endpoint, the SVM must have only FlexVol volumes. For a Consistency Group this property identifies the source Consistency Group name. When specifying a Consistency Group as the source endpoint, the "source.consistency_group_volumes" property must be specified with the FlexVol volumes of type "RW". FlexVol volumes of type "DP" cannot be specified in the "source.consistency_group_volumes" list."""

    state = fields.Str(
        data_key="state",
        validate=enum_validation(['broken_off', 'paused', 'snapmirrored', 'uninitialized', 'in_sync', 'out_of_sync', 'synchronizing', 'expanding']),
    )
    r""" State of the relationship.<br>To initialize the relationship, PATCH the state to "snapmirrored" for relationships with a policy of type "async" or to state "in_sync" for relationships with a policy of type "sync".<br>To break the relationship, PATCH the state to "broken_off" for relationships with a policy of type "async" or "sync". SnapMirror relationships with the policy type as "sync" and "sync_type" as "automated_failover" cannot be "broken_off".<br>To resync the relationship, PATCH the state to "snapmirrored" for relationships with a policy of type "async" or to state "in_sync" for relationships with a policy of type "sync". SnapMirror relationships with the policy type as "sync" and "sync_type" as "automated_failover" can be in "broken_off" state due to a failed attempt of SnapMirror failover.<br>To pause the relationship, suspending further transfers, PATCH the state to "paused" for relationships with a policy of type "async" or "sync". SnapMirror relationships with the policy type as "sync" and "sync_type" as "automated_failover" cannot be "paused".<br>To resume transfers for a paused relationship, PATCH the state to "snapmirrored" for relationships with a policy of type "async" or to state "in_sync" for relationships with a policy of type "sync".<br>The entries "in_sync", "out_of_sync", "synchronizing", and "expanding" are only applicable to relationships with a policy of type "sync". A PATCH call on the state change only triggers the transition to the specified state. You must poll on the "state", "healthy" and "unhealthy_reason" properties using a GET request to determine if the transition is successful. To automatically initialize the relationship when specifying "create_destination" property, set the state to "snapmirrored" for relationships with a policy of type "async" or to state "in_sync" for relationships with a policy of type "sync".

Valid choices:

* broken_off
* paused
* snapmirrored
* uninitialized
* in_sync
* out_of_sync
* synchronizing
* expanding"""

    svmdr_volumes = fields.List(fields.Nested("netapp_ontap.models.snapmirror_relationship_svmdr_volumes.SnapmirrorRelationshipSvmdrVolumesSchema", unknown=EXCLUDE), data_key="svmdr_volumes")
    r""" Specifies the list of constituent FlexVol volumes and FlexGroup volumes for an SVM DR SnapMirror relationship. FlexGroup constituents are not considered."""

    throttle = Size(
        data_key="throttle",
    )
    r""" Throttle, in KBs per second. This "throttle" overrides the "throttle" set on the SnapMirror relationship's policy. If neither of these are set, defaults to 0, which is interpreted as unlimited."""

    total_transfer_bytes = Size(
        data_key="total_transfer_bytes",
    )
    r""" Indicates the cumulative bytes transfered since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.

Example: 1098210312"""

    total_transfer_duration = fields.Str(
        data_key="total_transfer_duration",
    )
    r""" Indicates the cumulative duration of all transfers since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.

Example: PT3M21S"""

    transfer = fields.Nested("netapp_ontap.models.snapmirror_relationship_transfer.SnapmirrorRelationshipTransferSchema", data_key="transfer", unknown=EXCLUDE)
    r""" The transfer field of the snapmirror_relationship."""

    transfer_schedule = fields.Nested("netapp_ontap.resources.schedule.ScheduleSchema", data_key="transfer_schedule", unknown=EXCLUDE)
    r""" The transfer_schedule field of the snapmirror_relationship."""

    unhealthy_reason = fields.List(fields.Nested("netapp_ontap.models.snapmirror_error.SnapmirrorErrorSchema", unknown=EXCLUDE), data_key="unhealthy_reason")
    r""" Reason the relationship is not healthy. It is a concatenation of up to four levels of error messages.

Example: [{"code":"6621444","message":"Failed to complete update operation on one or more item relationships.","parameters":[]},{"code":"6621445","message":"Group Update failed","parameters":[]}]"""

    uuid = fields.Str(
        data_key="uuid",
    )
    r""" The uuid field of the snapmirror_relationship.

Example: 4ea7a442-86d1-11e0-ae1c-123478563412"""

    @property
    def resource(self):
        return SnapmirrorRelationship

    gettable_fields = [
        "links",
        "consistency_group_failover",
        "destination",
        "exported_snapshot",
        "group_type",
        "healthy",
        "identity_preservation",
        "lag_time",
        "last_transfer_network_compression_ratio",
        "last_transfer_type",
        "policy",
        "restore",
        "source",
        "state",
        "svmdr_volumes",
        "throttle",
        "total_transfer_bytes",
        "total_transfer_duration",
        "transfer",
        "transfer_schedule.links",
        "transfer_schedule.name",
        "transfer_schedule.uuid",
        "unhealthy_reason",
        "uuid",
    ]
    """links,consistency_group_failover,destination,exported_snapshot,group_type,healthy,identity_preservation,lag_time,last_transfer_network_compression_ratio,last_transfer_type,policy,restore,source,state,svmdr_volumes,throttle,total_transfer_bytes,total_transfer_duration,transfer,transfer_schedule.links,transfer_schedule.name,transfer_schedule.uuid,unhealthy_reason,uuid,"""

    patchable_fields = [
        "destination",
        "group_type",
        "identity_preservation",
        "last_transfer_network_compression_ratio",
        "last_transfer_type",
        "policy",
        "preserve",
        "quick_resync",
        "recover_after_break",
        "restore_to_snapshot",
        "source",
        "state",
        "throttle",
        "total_transfer_bytes",
        "total_transfer_duration",
        "transfer",
        "transfer_schedule.name",
        "transfer_schedule.uuid",
    ]
    """destination,group_type,identity_preservation,last_transfer_network_compression_ratio,last_transfer_type,policy,preserve,quick_resync,recover_after_break,restore_to_snapshot,source,state,throttle,total_transfer_bytes,total_transfer_duration,transfer,transfer_schedule.name,transfer_schedule.uuid,"""

    postable_fields = [
        "create_destination",
        "destination",
        "group_type",
        "identity_preservation",
        "last_transfer_network_compression_ratio",
        "last_transfer_type",
        "policy",
        "restore",
        "source",
        "state",
        "svmdr_volumes",
        "throttle",
        "total_transfer_bytes",
        "total_transfer_duration",
        "transfer",
        "transfer_schedule.name",
        "transfer_schedule.uuid",
    ]
    """create_destination,destination,group_type,identity_preservation,last_transfer_network_compression_ratio,last_transfer_type,policy,restore,source,state,svmdr_volumes,throttle,total_transfer_bytes,total_transfer_duration,transfer,transfer_schedule.name,transfer_schedule.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in SnapmirrorRelationship.get_collection(fields=field)]
    return getter

async def _wait_for_job(response: NetAppResponse) -> None:
    """Examine the given response. If it is a job, asynchronously wait for it to
    complete. While polling, prints the current status message of the job.
    """

    if not response.is_job:
        return
    from netapp_ontap.resources import Job
    job = Job(**response.http_response.json()["job"])
    while True:
        job.get(fields="state,message")
        if hasattr(job, "message"):
            print("[%s]: %s" % (job.state, job.message))
        if job.state == "failure":
            raise NetAppRestError("SnapmirrorRelationship modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class SnapmirrorRelationship(Resource):
    r""" SnapMirror relationship information. The SnapMirror relatiosnhip can be either "async" or "sync" based on the type of SnapMirror policy associated with the relationship. The source and destination endpoints of a SnapMirror relationship must be of the same type, for example, if the source endpoint is a FlexVol volume then the destination endpoint must be a FlexVol volume.<br>The SnapMirror policy type "async" can be used when the SnapMirror relationship has FlexVol volume or FlexGroup volume or SVM as the endpoint. The SnapMirror policy type "sync" can be used when the SnapMirror relationship has FlexVol volume as the endpoint. The SnapMirror policy type "sync" with "sync_type" as "automated_failover" can be used when the SnapMirror relationship has Consistency Group as the endpoint. """

    _schema = SnapmirrorRelationshipSchema
    _path = "/api/snapmirror/relationships"
    _keys = ["uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves information for SnapMirror relationships whose destination endpoints are in the current SVM or the current cluster, depending on the cluster context.
### Related ONTAP commands
* `snapmirror show`
* `snapmirror list-destinations`
### Expensive properties

* `source.consistency_group_volumes.name`
* `destination.consistency_group_volumes.name`
* `svmdr_volumes.name`
### Examples
The following examples show how to retrieve the list of SnapMirror relationships and the list of SnapMirror destinations.
   1. Retrieving the list of SnapMirror relationships. This API must be run on the cluster containing the destination endpoint.
   <br/>
   ```
   GET "/api/snapmirror/relationships/"
   ```
   <br/>
  2.  Retrieving the list of SnapMirror destinations on source. This must be run on the cluster containing the source endpoint.
   <br/>
   ```
   GET "/api/snapmirror/relationships/?list_destinations_only=true"
   ```
   <br/>
  3.  Retrieving the relationship UUID of SnapMirror relationships with lag time greater than 2 days. This API must be run on the cluster containing the destination endpoint.
   <br/>
   ```
   GET "/api/snapmirror/relationships/?fields=uuid&lag_time=>P2DT"
   ```
   <br/>
  4.  Retrieving the list of SnapMirror relationships with lag time less than 10 hours. This API must be run on the cluster containing the destination endpoint.
   <br/>
   ```
   GET "/api/snapmirror/relationships/?lag_time=<PT10H"
   ```
   <br/>
  
  6. Retrieving the list of constituent volumes for SVM DR Snapmirror relationships.
   <br/>
   ```
   GET "/api/snapmirror/relationships/?fields=svmdr_volumes.name"
   ```
   <br/>
  </private>
### Learn more
* [`DOC /snapmirror/relationships`](#docs-snapmirror-snapmirror_relationships)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="snapmirror relationship show")
        def snapmirror_relationship_show(
            fields: List[Choices.define(["exported_snapshot", "group_type", "healthy", "identity_preservation", "lag_time", "last_transfer_network_compression_ratio", "last_transfer_type", "preserve", "quick_resync", "recover_after_break", "restore", "restore_to_snapshot", "state", "throttle", "total_transfer_bytes", "total_transfer_duration", "uuid", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of SnapmirrorRelationship resources

            Args:
                exported_snapshot: Snapshot copy exported to clients on destination.
                group_type: Specifies the group type of the top level SnapMirror relationship. The volume relationships are shown as _none_, the SVMDR relationships are shown as _svm_dr_, the Consistency Group relationships are shown as _consistency_group_, and the FlexGroup volume relationships are shown as _flexgroup_.
                healthy: Is the relationship healthy?
                identity_preservation: Specifies which configuration of the source SVM is replicated to the destination SVM. This property is applicable only for SVM data protection with \"async\" policy type. This \"identity_preservation\" overrides the \"identity_preservation\" set on the SnapMirror relationship's policy.
                lag_time: Time since the exported Snapshot copy was created.
                last_transfer_network_compression_ratio: Specifies the compression ratio achieved for the data sent over the wire with network compression enabled for the last successful transfer.
                last_transfer_type: Specifies the operation type of the last transfer that occurred on the relationship. The _initialize_ transfer occurs when the relationship state changes from uninitialized to snapmirrored or in_sync. The _update_ transfer occurs when the snapshots are transferred from the source endpoint to the destination endpoint as part of scheduled or manual update. The _resync_ transfer occurs when the relationship state changes from broken_off to snapmirrored or in_sync. The _restore_ transfer occurs when the snapshot is restored from a destination endpoint to another endpoint.
                preserve: Set to true on resync to preserve Snapshot copies on the destination that are newer than the latest common Snapshot copy. This property is applicable only for relationships with FlexVol volume or FlexGroup volume endpoints and when the PATCH state is being changed to \"snapmirrored\".
                quick_resync: Set to true to reduce resync time by not preserving storage efficiency. This property is applicable only for relationships with FlexVol volume endpoints and SVMDR relationships when the PATCH state is being changed to \"snapmirrored\".
                recover_after_break: Set to true to recover from a failed SnapMirror break operation on a FlexGroup volume relationship. This restores all destination FlexGroup constituent volumes to the latest Snapshot copy, and any writes to the read-write constituents are lost. This property is applicable only for SnapMirror relationships with FlexGroup volume endpoints and when the PATCH state is being changed to \"broken_off\".
                restore: Set to true to create a relationship for restore. To trigger restore-transfer, use transfers POST on the restore relationship. SnapMirror relationships with the policy type \"async\" can be restored. SnapMirror relationships with the policy type \"sync\" cannot be restored.
                restore_to_snapshot: Specifies the Snapshot copy to restore to on the destination during the break operation. This property is applicable only for SnapMirror relationships with FlexVol volume endpoints and when the PATCH state is being changed to \"broken_off\".
                state: State of the relationship.<br>To initialize the relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".<br>To break the relationship, PATCH the state to \"broken_off\" for relationships with a policy of type \"async\" or \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" cannot be \"broken_off\".<br>To resync the relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" can be in \"broken_off\" state due to a failed attempt of SnapMirror failover.<br>To pause the relationship, suspending further transfers, PATCH the state to \"paused\" for relationships with a policy of type \"async\" or \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" cannot be \"paused\".<br>To resume transfers for a paused relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".<br>The entries \"in_sync\", \"out_of_sync\", \"synchronizing\", and \"expanding\" are only applicable to relationships with a policy of type \"sync\". A PATCH call on the state change only triggers the transition to the specified state. You must poll on the \"state\", \"healthy\" and \"unhealthy_reason\" properties using a GET request to determine if the transition is successful. To automatically initialize the relationship when specifying \"create_destination\" property, set the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".
                throttle: Throttle, in KBs per second. This \"throttle\" overrides the \"throttle\" set on the SnapMirror relationship's policy. If neither of these are set, defaults to 0, which is interpreted as unlimited.
                total_transfer_bytes: Indicates the cumulative bytes transfered since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.
                total_transfer_duration: Indicates the cumulative duration of all transfers since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.
                uuid: 
            """

            kwargs = {}
            if exported_snapshot is not None:
                kwargs["exported_snapshot"] = exported_snapshot
            if group_type is not None:
                kwargs["group_type"] = group_type
            if healthy is not None:
                kwargs["healthy"] = healthy
            if identity_preservation is not None:
                kwargs["identity_preservation"] = identity_preservation
            if lag_time is not None:
                kwargs["lag_time"] = lag_time
            if last_transfer_network_compression_ratio is not None:
                kwargs["last_transfer_network_compression_ratio"] = last_transfer_network_compression_ratio
            if last_transfer_type is not None:
                kwargs["last_transfer_type"] = last_transfer_type
            if preserve is not None:
                kwargs["preserve"] = preserve
            if quick_resync is not None:
                kwargs["quick_resync"] = quick_resync
            if recover_after_break is not None:
                kwargs["recover_after_break"] = recover_after_break
            if restore is not None:
                kwargs["restore"] = restore
            if restore_to_snapshot is not None:
                kwargs["restore_to_snapshot"] = restore_to_snapshot
            if state is not None:
                kwargs["state"] = state
            if throttle is not None:
                kwargs["throttle"] = throttle
            if total_transfer_bytes is not None:
                kwargs["total_transfer_bytes"] = total_transfer_bytes
            if total_transfer_duration is not None:
                kwargs["total_transfer_duration"] = total_transfer_duration
            if uuid is not None:
                kwargs["uuid"] = uuid
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return SnapmirrorRelationship.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all SnapmirrorRelationship resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["SnapmirrorRelationship"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a SnapMirror relationship. This API is used to initiate SnapMirror operations such as "initialize", "resync", "break", "quiesce", and "resume" by specifying the appropriate value for the "state" field. It is also used to modify the SnapMirror policy associated with the specified relationship. Additionally, a SnapMirror relationship can be failed over to the destination endpoint or a failed over SnapMirror relationship can be failed back to the original state or a SnapMirror relationship direction can be reversed using this API. This API can also be used to expand the SnapMirror Business Continuity relationship with the specified source and destination volumes.
<br>To initialize the relationship, PATCH the state to "snapmirrored" for relationships with a policy of type "async" or "in_sync" for relationships with a policy of type "sync".
<br>To break the relationship or to failover to the destination endpoint and start serving data from the destination endpoint, PATCH the state to "broken_off" for relationships with a policy of type "async" or "sync". SnapMirror relationships with the policy type as "sync" and sync_type as "automated_failover" cannot be "broken_off".
<br>To resync the broken relationship, PATCH the state to "snapmirrored" for relationships with a policy of type "async" or "in_sync" for relationships with a policy of type "sync".
<br>To failback the failed over relationship and start serving data from the source endpoint, PATCH the state to "snapmirrored" for relationships with a policy of type "async" or "in_sync" for relationships with a policy of type "sync" and set the query flag "failback" as "true". SnapMirror relationships with the policy type as "sync" and sync_type as "automated_failover" can be in "broken_off" state due to a failed attempt of automated SnapMirror failover operation.
<br>To pause the relationship, suspending further transfers, PATCH the state to "paused" for relationships with a policy of type "async" or "sync". SnapMirror relationships with the policy type as "sync" and sync_type as "automated_failover" cannot be "paused".
<br>To resume transfers for a paused relationship, PATCH the state to "snapmirrored" for relationships with a policy of type "async" or "in_sync" for relationships with a policy of type "sync".
<br>To reverse the direction of the relationship, PATCH the "source.path" with the destination endpoint and the "destination.path" with the source endpoint and the relationship state to "snapmirrored" for relationships with a policy of type "async" or "in_sync" for relationships with a policy of type "sync". For relationships with a policy of type "async" and relationship state as "snapmirrored", stop IO on the source endpoint and perform a SnapMirror transfer POST operation before reversing the direction of the relationship to prevent any loss of data.
<br>The values "in_sync", "out_of_sync", and "synchronizing" are only applicable to relationships with a policy of type "sync".
<br>When "transfer_schedule" is specified along with "state" during PATCH, first the schedule is modified on the relationship and then the respective SnapMirror operation is initiated. The "transfer_schedule" specified is used to update asynchronous relationships.
<br>When "throttle" is specified along with "state" during PATCH, first the throttle is modified on the relationship, which will be used by any upcoming transfers and then the respective SnapMirror operation is initiated. If the SnapMirror operation initiated a transfer then it will also use the new throttle. If "throttle" needs to be applied for a specific transfer use SnapMirror Transfer REST API.
<br>For SnapMirror Business Continuity (SMBC) relationships, when "consistency_group_volumes" is specified during PATCH, first the existing FlexVol volume relationship is deleted and released and then the volumes are added to the SMBC relationship.

### Examples
### Related ONTAP commands
* `snapmirror modify`
* `snapmirror initialize`
* `snapmirror resync`
* `snapmirror break`
* `snapmirror quiesce`
* `snapmirror resume`
### Important notes
* The property "transfer_schedule" if set on a SnapMirror relationship overrides the "transfer_schedule" set on the policy being used with the SnapMirror relationship.
* The property "throttle" if set on a SnapMirror relationship overrides the "throttle" set on the policy being used with the SnapMirror relationship.
* The properties "transfer_schedule" and "throttle" are not supported when "failback" is set to "true".
* The properties "transfer_schedule" and "throttle" are not supported when "failover" is set to "true".
* The properties "transfer_schedule" and "throttle" are not supported when "force_failover" is set to "true".
* The properties "transfer_schedule" and "throttle" are not supported when the direction of the relationship is being reversed.
* To remove a transfer_schedule on a SnapMirror relationship set the "transfer_schedule" to null (no-quotes) during SnapMirror relationship PATCH.
* The property "identity_preservation" value can be changed from a higher "identity_preservation" threshold value to a lower "identity_preservation" threshold value but not vice-versa. For example, the threshold value of the "identity_preservation" property can be changed from "full" to "exclude_network_config", but cannot be increased from "exclude_network_and_protocol_config" to "exclude_network_config" to "full". The threshold value of the "identity_preservation" cannot be changed to "exclude_network_and_protocol_config" for IDP SVMDR.

### Examples
The following examples show how to perform the SnapMirror "resync", "initialize", "resume", "quiesce", and "break" operations. In addition, a relationship can be failed over to the destination endpoint and start serving data from the destination endpoint. A failed over relationship can be failed back to the source endpoint and serve data from the source endpoint. Also a relationship can be reversed by making the source endpoint as the new destination endpoint and the destination endpoint as the new source endpoint.
<br/>
   To update an associated SnapMirror policy.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"policy": { "name" : "MirrorAndVaultDiscardNetwork"}}'
   ```
   <br/>
   To perform SnapMirror "resync" for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored"}'
   ```
   <br/>
   To perform SnapMirror "initialize" for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored"}'
   ```
   <br/>
   To perform SnapMirror "resume" for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored"}'
   ```
   <br/>
   To perform SnapMirror "quiesce" for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff" '{"state":"paused"}'
   ```
   <br/>
   To perform SnapMirror "break" for an asynchronous SnapMirror relationship. This operation does a failover to the destination endpoint. After a the failover, data can then be served from the destination endpoint.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff" '{"state":"broken_off"}'
   ```
   <br/>
   To forcefully failover to the destination endpoint and start serving data from the destination endpoint.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/?force=true" '{"state":"broken_off"}'
   ```
   <br/>
   To failback to the source endpoint and start serving data from the source endpoint for an asynchronous relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/?failback=true" '{"state":"snapmirrored"}'
   ```
   <br/>
   To failback to the source endpoint and start serving data from the source endpoint for a synchronous relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/?failback=true" '{"state":"in_sync"}'
   ```
   <br/>
   To reverse the direction of an asynchronous relationship, that is, make the source endpoint as the new destination endpoint and make the destination endpoint as the new source endpoint.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"source": {"path": "dst_svm:dst_vol"}, "destination": {"path": "src_svm:src_vol"}, "state": "snapmirrored"}'
   ```
   <br/>
   To reverse the direction of a synchronous relationship, that is, make the source endpoint as the new destination endpoint and make the destination endpoint as the new source endpoint.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"source": {"path": "dst_svm:dst_vol"}, "destination": {"path": "src_svm:src_vol"}, "state": "in_sync"}'
   ```
   <br/>
   Updating SnapMirror transfer_schedule and throttle for an asynchronous SnapMirror relationship. Transfer_schedule can be specified as UUID or name or both.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"transfer_schedule":{"uuid":"817500fa-092d-44c5-9c10-7b54f7b2f20a", "name":"5min"}, "throttle":100}'
   ```
   <br/>
   Removing the SnapMirror transfer_schedule for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"transfer_schedule":{"uuid":null, "name":null}}'
   ```
   <br/>
   Removing the SnapMirror throttle for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"throttle":0}'
   ```
   <br/>
   To perform SnapMirror "resync" and update the SnapMirror transfer_schedule for an asynchronous SnapMirror relationship. First the transfer_schedule is modified and then the resync transfer is initiated.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored", transfer_schedule":{"uuid":"817500fa-092d-44c5-9c10-7b54f7b2f20a", "name":"5min"}}'
   ```
   <br/>
   To perform SnapMirror "initialize" and update the SnapMirror throttle for an asynchronous SnapMirror relationship. First the throttle is modified and then the initialize transfer is initiated. The initialize transfer will use this new throttle.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored", "throttle":100}'
   ```
   <br/>
   To perform SnapMirror "resync" and update the SnapMirror throttle for an asynchronous SnapMirror relationship. First the throttle is modified and then the resync transfer is initiated. The resync transfer will use this new throttle.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored", "throttle":100}'
   ```
   <br/>
   To perform a SnapMirror Business Continuity expansion.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{ "source" : {"consistency_group_volumes":[{"name":"vol"}]}, "destination" : {"consistency_group_volumes":[{"name":"voldp"}]} }'
### Learn more
* [`DOC /snapmirror/relationships`](#docs-snapmirror-snapmirror_relationships)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["SnapmirrorRelationship"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["SnapmirrorRelationship"], NetAppResponse]:
        r"""Creates a SnapMirror relationship. This API can optionally provision the destination endpoint when it does not exist. This API must be executed on the cluster containing the destination endpoint unless the destination endpoint is being provisioned. When the destination endpoint is being provisioned, this API can also be executed from the cluster containing the source endpoint. Provisioning of the destination endpoint from the source cluster is supported for the FlexVol volume, FlexGroup volume and Application Consistency Group endpoints.<br/>
For SVM endpoints, provisioning the destination SVM endpoint is not supported from the source cluster. When the destination endpoint exists, the source SVM and the destination SVM must be in an SVM peer relationship. When provisioning the destination endpoint, the SVM peer relationship between the source SVM and the destination SVM is established as part of the destination provision, provided that the source SVM has SVM peering permissions for the destination cluster.

### Required properties
* `source.path` - Path to the source endpoint of the SnapMirror relationship.
* `destination.path` - Path to the destination endpoint of the SnapMirror relationship.
* `source.consistency_group_volumes` - List of FlexVol volumes of type "RW" that are constituents of an Application Consistency Group.
* `destination.consistency_group_volumes` - List of FlexVol volumes of type "DP" that are constituents of an Application Consistency Group.
### Recommended optional properties
* `policy.name` or `policy.uuid` - Policy governing the SnapMirror relationship.
* `state` - Set the state to "snapmirrored" to automatically initialize the relationship.
* `create_destination.enabled` - Enable this property to provision the destination endpoint.

### Default property values
If not specified in POST, the following default property values are assigned:
* `policy.name` - _Asynchronous_
* `restore` - _false_
* `create_destination.tiering.policy` - `_snapshot_only_` (when `create_destination.tiering.supported` is _true_ for FlexVol volume)
* `create_destination.tiering.policy` - `_none_` (when `create_destination.tiering.supported` is _true_ for FlexGroup volume)
* `create_destination.storage_service.enforce_performance` - `_false_`
* `source.ipspace` - `_Default_`
* `destination.ipspace` - `_Default_`
* `throttle` - _0_
### Related ONTAP commands
* `snapmirror create`
* `snapmirror protect`
### Important notes
* The property "transfer_schedule" if set on a SnapMirror relationship overrides the "transfer_schedule" set on the policy being used with the SnapMirror relationship.
* The property "throttle" if set on a SnapMirror relationship overrides the "throttle" set on the policy being used with the SnapMirror relationship.
* The properties "transfer_schedule" and "throttle" are not supported when "restore" is set to "true".
* The property "transfer_schedule" cannot be set to null (no-quotes) during SnapMirror relationship POST.
* The property "throttle" is not supported when "create_destination.enabled" is set to "true".
* The property "identity_preservation" is applicable to only SnapMirror relationships with SVM endpoints and it indicates which configuration of the source SVM is replicated to the destination SVM.
### Examples
The following examples show how to create FlexVol volumes, FlexGroup volumes, SVM and Application Consistency Group SnapMirror relationships. Note that the source SVM name must be the local name of the peer SVM.</br>
   Creating a FlexVol SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}}'
   ```
   <br/>
   Creating a FlexGroup SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:source_flexgrp"}, "destination": { "path": "dst_svm:dest_flexgrp"}}'
   ```
   <br/>
   Creating a SVM SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": { "path": "src_svm:"}, "destination": { "path": "dst_svm:"}}'
   ```
   <br/>
   Creating a SnapMirror relationship in order to restore from a destination.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}, "restore": "true"}'
   ```
   <br/>
   Provision the destination FlexVol volume endpoint and create a SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}, "create_destination": { "enabled": "true" }}'
   ```
   Provision the destination FlexVol volume endpoint on a Fabricpool with a tiering policy and create a SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}, "create_destination": { "enabled": "true", "tiering": { "supported": "true", "policy": "auto" } } }'
   ```
   Provision the destination FlexVol volume endpoint using storage service and create a SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}, "create_destination": { "enabled": "true", "storage_service": { "enabled": "true", "name": "extreme", "enforce_performance": "true" } } }'
   ```
   Provision the destination SVM endpoint and create a SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:", "cluster": { "name": "cluster_src" }}, "destination": { "path": "dst_svm:"}, "create_destination": { "enabled: "true" }}'
   ```
   Create an asynchronous SnapMirror relationship with Application Consistency Group endpoint.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": { "path": "src_svm:/cg/cg_src_vol", "consistency_group_volumes": [{ "name": "src_vol_1" }, { "name": "src_vol_2" }] }, "destination": { "path": "dst_svm:/cg/cg_dst_vol", "consistency_group_volumes": [{ "name": "dst_vol_1" }, { "name": "dst_vol_2" }] }, "policy": "Asynchronous" }'
   ```
   Provision the destination Application Consistency Group endpoint on a Fabricpool with a tiering policy, create an asynchronous SnapMirror relationship with a SnapMirror policy of type "async", and initialize the SnapMirror relationship with state as "snapmirrored".
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:/cg/cg_src_vol", "consistency_group_volumes": [{ "name": "src_vol_1" }, { "name": "src_vol_2" }] }, "destination": { "path": "dst_svm:/cg/cg_dst_vol", "consistency_group_volumes": [{ "name": "dst_vol_1" }, { "name": "dst_vol_2" }] }, "create_destination": { "enabled": "true", "tiering": { "supported": "true" } }, "policy": "Asynchronous", "state": "snapmirrored" }'
   ```
   Create a SnapMirror Business Continuity (SMBC) relationship with the Application Consistency Group endpoint.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": { "path": "src_svm:/cg/cg_src_vol", "consistency_group_volumes": [{ "name": "src_vol_1" }, { "name": "src_vol_2" }] }, "destination": { "path": "dst_svm:/cg/cg_dst_vol", "consistency_group_volumes": [{ "name": "dst_vol_1" }, { "name": "dst_vol_2" }] }, "policy": "AutomatedFailOver" }'
   ```
   Provision the destination Application Consistency Group endpoint on a Fabricpool with a tiering policy, create a SnapMirror Business Continuity (SMBC) relationship with a SnapMirror policy of type "sync" and sync_type of "automated_failover", and initialize the SnapMirror relationship with state as "in_sync".
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:/cg/cg_src_vol", "consistency_group_volumes": [{ "name": "src_vol_1" }, { "name": "src_vol_2" }] }, "destination": { "path": "dst_svm:/cg/cg_dst_vol", "consistency_group_volumes": [{ "name": "dst_vol_1" }, { "name": "dst_vol_2" }] }, "create_destination": { "enabled": "true", "tiering": { "supported": "true" } }, "policy": "AutomatedFailOver", "state": "in_sync" }'
   ```
   Provision the destination Application Consistency Group endpoint with storage service, create a SnapMirror Business Continuity (SMBC) relationship with a SnapMirror policy of type "sync" and sync_type of "automated_failover", and initialize the SnapMirror relationship with state as "in_sync".
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:/cg/cg_src_vol", "consistency_group_volumes": [{ "name": "src_vol_1" }, { "name": "src_vol_2" }] }, "destination": { "path": "dst_svm:/cg/cg_dst_vol", "consistency_group_volumes": [{ "name": "dst_vol_1" }, { "name": "dst_vol_2" }] }, "create_destination": { "enabled": "true", "storage_service": { "enabled": "true", "name": "extreme", "enforce_performance": "true" } }, "policy": "AutomatedFailOver", "state": "in_sync" }'
   ```
   <br/>
   Creating a FlexVol volume SnapMirror relationship of type XDP with transfer_schedule and throttle.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}, "transfer_schedule":{"uuid":"817500fa-092d-44c5-9c10-7b54f7b2f20a", "name":"5min"}, "throttle":100}'
   ```
   <br/>
   
### Learn more
* [`DOC /snapmirror/relationships`](#docs-snapmirror-snapmirror_relationships)
"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["SnapmirrorRelationship"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a SnapMirror relationship.
### Important notes
* The "destination_only", "source_only", and "source_info_only" flags are mutually exclusive. If no flag is specified, the relationship is deleted from both the source and destination and all common Snapshot copies between the source and destination are also deleted.
* For a restore relationship, the call must be executed on the cluster containing the destination endpoint without specifying the destination_only, source_only, or source_info_only parameters.
* Additionally, ensure that there are no ongoing transfers on a restore relationship before calling this API.
* The "failover", "force-failover" and "failback" query parameters are only applicable for SVM-DR SnapMirror relationships.
* When a SnapMirror relationship associated with a pair of source and destination Consistency Groups is deleted, the corresponding Consistency Groups on the source and destination clusters are not automatically deleted and remain in place.
### Related ONTAP commands
* `snapmirror delete`
* `snapmirror release`
### Examples
The following examples show how to delete the relationship from both the source and destination, the destination only, and the source only.
<br/>
   Deleting the relationship from both the source and destination. This API must be run on the cluster containing the destination endpoint.
   <br/>
   ```
   DELETE "/api/snapmirror/relationships/4512b2d2-fd60-11e8-8929-005056bbfe52"
   ```
   <br/>
   Deleting the relationship on the destination only. This API must be run on the cluster containing the destination endpoint.
   <br/>
   ```
   DELETE "/api/snapmirror/relationships/fd1e0697-02ba-11e9-acc7-005056a7697f/?destination_only=true"
   ```
   <br/>
   Deleting the relationship on the source only. This API must be run on the cluster containing the source endpoint.
   <br/>
   ```
   DELETE "/api/snapmirror/relationships/93e828ba-02bc-11e9-acc7-005056a7697f/?source_only=true"
   ```
   <br/>
   Deleting the source information only. This API must be run on the cluster containing the source endpoint. This does not delete the common Snapshot copies between the source and destination.
   <br/>
   ```
   DELETE "/api/snapmirror/relationships/caf545a2-fc60-11e8-aa13-005056a707ff/?source_info_only=true"
   ```
   <br/>
### Learn more
* [`DOC /snapmirror/relationships`](#docs-snapmirror-snapmirror_relationships)
"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves information for SnapMirror relationships whose destination endpoints are in the current SVM or the current cluster, depending on the cluster context.
### Related ONTAP commands
* `snapmirror show`
* `snapmirror list-destinations`
### Expensive properties

* `source.consistency_group_volumes.name`
* `destination.consistency_group_volumes.name`
* `svmdr_volumes.name`
### Examples
The following examples show how to retrieve the list of SnapMirror relationships and the list of SnapMirror destinations.
   1. Retrieving the list of SnapMirror relationships. This API must be run on the cluster containing the destination endpoint.
   <br/>
   ```
   GET "/api/snapmirror/relationships/"
   ```
   <br/>
  2.  Retrieving the list of SnapMirror destinations on source. This must be run on the cluster containing the source endpoint.
   <br/>
   ```
   GET "/api/snapmirror/relationships/?list_destinations_only=true"
   ```
   <br/>
  3.  Retrieving the relationship UUID of SnapMirror relationships with lag time greater than 2 days. This API must be run on the cluster containing the destination endpoint.
   <br/>
   ```
   GET "/api/snapmirror/relationships/?fields=uuid&lag_time=>P2DT"
   ```
   <br/>
  4.  Retrieving the list of SnapMirror relationships with lag time less than 10 hours. This API must be run on the cluster containing the destination endpoint.
   <br/>
   ```
   GET "/api/snapmirror/relationships/?lag_time=<PT10H"
   ```
   <br/>
  
  6. Retrieving the list of constituent volumes for SVM DR Snapmirror relationships.
   <br/>
   ```
   GET "/api/snapmirror/relationships/?fields=svmdr_volumes.name"
   ```
   <br/>
  </private>
### Learn more
* [`DOC /snapmirror/relationships`](#docs-snapmirror-snapmirror_relationships)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a SnapMirror relationship.
### Related ONTAP commands
* `snapmirror show`
* `snapmirror list-destinations`
### Expensive properties

* `source.consistency_group_volumes.name`
* `destination.consistency_group_volumes.name`
### Example
<br/>
```
GET "/api/snapmirror/relationships/caf545a2-fc60-11e8-aa13-005056a707ff/"
```
<br/>
### Learn more
* [`DOC /snapmirror/relationships`](#docs-snapmirror-snapmirror_relationships)
"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)

    def post(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Creates a SnapMirror relationship. This API can optionally provision the destination endpoint when it does not exist. This API must be executed on the cluster containing the destination endpoint unless the destination endpoint is being provisioned. When the destination endpoint is being provisioned, this API can also be executed from the cluster containing the source endpoint. Provisioning of the destination endpoint from the source cluster is supported for the FlexVol volume, FlexGroup volume and Application Consistency Group endpoints.<br/>
For SVM endpoints, provisioning the destination SVM endpoint is not supported from the source cluster. When the destination endpoint exists, the source SVM and the destination SVM must be in an SVM peer relationship. When provisioning the destination endpoint, the SVM peer relationship between the source SVM and the destination SVM is established as part of the destination provision, provided that the source SVM has SVM peering permissions for the destination cluster.

### Required properties
* `source.path` - Path to the source endpoint of the SnapMirror relationship.
* `destination.path` - Path to the destination endpoint of the SnapMirror relationship.
* `source.consistency_group_volumes` - List of FlexVol volumes of type "RW" that are constituents of an Application Consistency Group.
* `destination.consistency_group_volumes` - List of FlexVol volumes of type "DP" that are constituents of an Application Consistency Group.
### Recommended optional properties
* `policy.name` or `policy.uuid` - Policy governing the SnapMirror relationship.
* `state` - Set the state to "snapmirrored" to automatically initialize the relationship.
* `create_destination.enabled` - Enable this property to provision the destination endpoint.

### Default property values
If not specified in POST, the following default property values are assigned:
* `policy.name` - _Asynchronous_
* `restore` - _false_
* `create_destination.tiering.policy` - `_snapshot_only_` (when `create_destination.tiering.supported` is _true_ for FlexVol volume)
* `create_destination.tiering.policy` - `_none_` (when `create_destination.tiering.supported` is _true_ for FlexGroup volume)
* `create_destination.storage_service.enforce_performance` - `_false_`
* `source.ipspace` - `_Default_`
* `destination.ipspace` - `_Default_`
* `throttle` - _0_
### Related ONTAP commands
* `snapmirror create`
* `snapmirror protect`
### Important notes
* The property "transfer_schedule" if set on a SnapMirror relationship overrides the "transfer_schedule" set on the policy being used with the SnapMirror relationship.
* The property "throttle" if set on a SnapMirror relationship overrides the "throttle" set on the policy being used with the SnapMirror relationship.
* The properties "transfer_schedule" and "throttle" are not supported when "restore" is set to "true".
* The property "transfer_schedule" cannot be set to null (no-quotes) during SnapMirror relationship POST.
* The property "throttle" is not supported when "create_destination.enabled" is set to "true".
* The property "identity_preservation" is applicable to only SnapMirror relationships with SVM endpoints and it indicates which configuration of the source SVM is replicated to the destination SVM.
### Examples
The following examples show how to create FlexVol volumes, FlexGroup volumes, SVM and Application Consistency Group SnapMirror relationships. Note that the source SVM name must be the local name of the peer SVM.</br>
   Creating a FlexVol SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}}'
   ```
   <br/>
   Creating a FlexGroup SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:source_flexgrp"}, "destination": { "path": "dst_svm:dest_flexgrp"}}'
   ```
   <br/>
   Creating a SVM SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": { "path": "src_svm:"}, "destination": { "path": "dst_svm:"}}'
   ```
   <br/>
   Creating a SnapMirror relationship in order to restore from a destination.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}, "restore": "true"}'
   ```
   <br/>
   Provision the destination FlexVol volume endpoint and create a SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}, "create_destination": { "enabled": "true" }}'
   ```
   Provision the destination FlexVol volume endpoint on a Fabricpool with a tiering policy and create a SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}, "create_destination": { "enabled": "true", "tiering": { "supported": "true", "policy": "auto" } } }'
   ```
   Provision the destination FlexVol volume endpoint using storage service and create a SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}, "create_destination": { "enabled": "true", "storage_service": { "enabled": "true", "name": "extreme", "enforce_performance": "true" } } }'
   ```
   Provision the destination SVM endpoint and create a SnapMirror relationship of type XDP.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:", "cluster": { "name": "cluster_src" }}, "destination": { "path": "dst_svm:"}, "create_destination": { "enabled: "true" }}'
   ```
   Create an asynchronous SnapMirror relationship with Application Consistency Group endpoint.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": { "path": "src_svm:/cg/cg_src_vol", "consistency_group_volumes": [{ "name": "src_vol_1" }, { "name": "src_vol_2" }] }, "destination": { "path": "dst_svm:/cg/cg_dst_vol", "consistency_group_volumes": [{ "name": "dst_vol_1" }, { "name": "dst_vol_2" }] }, "policy": "Asynchronous" }'
   ```
   Provision the destination Application Consistency Group endpoint on a Fabricpool with a tiering policy, create an asynchronous SnapMirror relationship with a SnapMirror policy of type "async", and initialize the SnapMirror relationship with state as "snapmirrored".
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:/cg/cg_src_vol", "consistency_group_volumes": [{ "name": "src_vol_1" }, { "name": "src_vol_2" }] }, "destination": { "path": "dst_svm:/cg/cg_dst_vol", "consistency_group_volumes": [{ "name": "dst_vol_1" }, { "name": "dst_vol_2" }] }, "create_destination": { "enabled": "true", "tiering": { "supported": "true" } }, "policy": "Asynchronous", "state": "snapmirrored" }'
   ```
   Create a SnapMirror Business Continuity (SMBC) relationship with the Application Consistency Group endpoint.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": { "path": "src_svm:/cg/cg_src_vol", "consistency_group_volumes": [{ "name": "src_vol_1" }, { "name": "src_vol_2" }] }, "destination": { "path": "dst_svm:/cg/cg_dst_vol", "consistency_group_volumes": [{ "name": "dst_vol_1" }, { "name": "dst_vol_2" }] }, "policy": "AutomatedFailOver" }'
   ```
   Provision the destination Application Consistency Group endpoint on a Fabricpool with a tiering policy, create a SnapMirror Business Continuity (SMBC) relationship with a SnapMirror policy of type "sync" and sync_type of "automated_failover", and initialize the SnapMirror relationship with state as "in_sync".
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:/cg/cg_src_vol", "consistency_group_volumes": [{ "name": "src_vol_1" }, { "name": "src_vol_2" }] }, "destination": { "path": "dst_svm:/cg/cg_dst_vol", "consistency_group_volumes": [{ "name": "dst_vol_1" }, { "name": "dst_vol_2" }] }, "create_destination": { "enabled": "true", "tiering": { "supported": "true" } }, "policy": "AutomatedFailOver", "state": "in_sync" }'
   ```
   Provision the destination Application Consistency Group endpoint with storage service, create a SnapMirror Business Continuity (SMBC) relationship with a SnapMirror policy of type "sync" and sync_type of "automated_failover", and initialize the SnapMirror relationship with state as "in_sync".
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:/cg/cg_src_vol", "consistency_group_volumes": [{ "name": "src_vol_1" }, { "name": "src_vol_2" }] }, "destination": { "path": "dst_svm:/cg/cg_dst_vol", "consistency_group_volumes": [{ "name": "dst_vol_1" }, { "name": "dst_vol_2" }] }, "create_destination": { "enabled": "true", "storage_service": { "enabled": "true", "name": "extreme", "enforce_performance": "true" } }, "policy": "AutomatedFailOver", "state": "in_sync" }'
   ```
   <br/>
   Creating a FlexVol volume SnapMirror relationship of type XDP with transfer_schedule and throttle.
   <br/>
   ```
   POST "/api/snapmirror/relationships/" '{"source": {"path": "src_svm:src_vol"}, "destination": { "path": "dst_svm:dst_vol"}, "transfer_schedule":{"uuid":"817500fa-092d-44c5-9c10-7b54f7b2f20a", "name":"5min"}, "throttle":100}'
   ```
   <br/>
   
### Learn more
* [`DOC /snapmirror/relationships`](#docs-snapmirror-snapmirror_relationships)
"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="snapmirror relationship create")
        async def snapmirror_relationship_create(
        ) -> ResourceTable:
            """Create an instance of a SnapmirrorRelationship resource

            Args:
                links: 
                consistency_group_failover: 
                create_destination: 
                destination: This property is the destination endpoint of the relationship. The destination endpoint can be a FlexVol volume, FlexGroup volume, Consistency Group, or SVM. For the POST request, the destination endpoint must be of type \"DP\" when the endpoint is a FlexVol volume or a FlexGroup volume. When specifying a Consistency Group as the destination endpoint, the \"destination.consistency_group_volumes\" property must be specified with the FlexVol volumes of type \"DP\". The POST request for SVM must have a destination endpoint of type \"dp-destination\". The destination endpoint path name must be specified in the \"destination.path\" property. For relationships of type \"async\", the destination endpoint for FlexVol volume and FlexGroup volume will change to type \"RW\" when the relationship status is \"broken_off\" and will revert to type \"DP\" when the relationship status is \"snapmirrored\" or \"in_sync\" using the PATCH request. The destination endpoint for SVM will change from \"dp-destination\" to type \"default\" when the relationship status is \"broken_off\" and will revert to type \"dp-destination\" when the relationship status is \"snapmirrored\" using the PATCH request. When the destination endpoint is a Consistency Group, the Consistency Group FlexVol volumes will change to type \"RW\" when the relationship status is \"broken_off\" and will revert to type \"DP\" when the relationship status is \"in_sync\" using the PATCH request.
                exported_snapshot: Snapshot copy exported to clients on destination.
                group_type: Specifies the group type of the top level SnapMirror relationship. The volume relationships are shown as _none_, the SVMDR relationships are shown as _svm_dr_, the Consistency Group relationships are shown as _consistency_group_, and the FlexGroup volume relationships are shown as _flexgroup_.
                healthy: Is the relationship healthy?
                identity_preservation: Specifies which configuration of the source SVM is replicated to the destination SVM. This property is applicable only for SVM data protection with \"async\" policy type. This \"identity_preservation\" overrides the \"identity_preservation\" set on the SnapMirror relationship's policy.
                lag_time: Time since the exported Snapshot copy was created.
                last_transfer_network_compression_ratio: Specifies the compression ratio achieved for the data sent over the wire with network compression enabled for the last successful transfer.
                last_transfer_type: Specifies the operation type of the last transfer that occurred on the relationship. The _initialize_ transfer occurs when the relationship state changes from uninitialized to snapmirrored or in_sync. The _update_ transfer occurs when the snapshots are transferred from the source endpoint to the destination endpoint as part of scheduled or manual update. The _resync_ transfer occurs when the relationship state changes from broken_off to snapmirrored or in_sync. The _restore_ transfer occurs when the snapshot is restored from a destination endpoint to another endpoint.
                policy: 
                preserve: Set to true on resync to preserve Snapshot copies on the destination that are newer than the latest common Snapshot copy. This property is applicable only for relationships with FlexVol volume or FlexGroup volume endpoints and when the PATCH state is being changed to \"snapmirrored\".
                quick_resync: Set to true to reduce resync time by not preserving storage efficiency. This property is applicable only for relationships with FlexVol volume endpoints and SVMDR relationships when the PATCH state is being changed to \"snapmirrored\".
                recover_after_break: Set to true to recover from a failed SnapMirror break operation on a FlexGroup volume relationship. This restores all destination FlexGroup constituent volumes to the latest Snapshot copy, and any writes to the read-write constituents are lost. This property is applicable only for SnapMirror relationships with FlexGroup volume endpoints and when the PATCH state is being changed to \"broken_off\".
                restore: Set to true to create a relationship for restore. To trigger restore-transfer, use transfers POST on the restore relationship. SnapMirror relationships with the policy type \"async\" can be restored. SnapMirror relationships with the policy type \"sync\" cannot be restored.
                restore_to_snapshot: Specifies the Snapshot copy to restore to on the destination during the break operation. This property is applicable only for SnapMirror relationships with FlexVol volume endpoints and when the PATCH state is being changed to \"broken_off\".
                source: This property is the source endpoint of the relationship. The source endpoint can be a FlexVol volume, FlexGroup volume, Consistency Group, or SVM. To establish a SnapMirror relationship with SVM as source endpoint, the SVM must have only FlexVol volumes. For a Consistency Group this property identifies the source Consistency Group name. When specifying a Consistency Group as the source endpoint, the \"source.consistency_group_volumes\" property must be specified with the FlexVol volumes of type \"RW\". FlexVol volumes of type \"DP\" cannot be specified in the \"source.consistency_group_volumes\" list.
                state: State of the relationship.<br>To initialize the relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".<br>To break the relationship, PATCH the state to \"broken_off\" for relationships with a policy of type \"async\" or \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" cannot be \"broken_off\".<br>To resync the relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" can be in \"broken_off\" state due to a failed attempt of SnapMirror failover.<br>To pause the relationship, suspending further transfers, PATCH the state to \"paused\" for relationships with a policy of type \"async\" or \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" cannot be \"paused\".<br>To resume transfers for a paused relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".<br>The entries \"in_sync\", \"out_of_sync\", \"synchronizing\", and \"expanding\" are only applicable to relationships with a policy of type \"sync\". A PATCH call on the state change only triggers the transition to the specified state. You must poll on the \"state\", \"healthy\" and \"unhealthy_reason\" properties using a GET request to determine if the transition is successful. To automatically initialize the relationship when specifying \"create_destination\" property, set the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".
                svmdr_volumes: Specifies the list of constituent FlexVol volumes and FlexGroup volumes for an SVM DR SnapMirror relationship. FlexGroup constituents are not considered.
                throttle: Throttle, in KBs per second. This \"throttle\" overrides the \"throttle\" set on the SnapMirror relationship's policy. If neither of these are set, defaults to 0, which is interpreted as unlimited.
                total_transfer_bytes: Indicates the cumulative bytes transfered since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.
                total_transfer_duration: Indicates the cumulative duration of all transfers since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.
                transfer: 
                transfer_schedule: 
                unhealthy_reason: Reason the relationship is not healthy. It is a concatenation of up to four levels of error messages.
                uuid: 
            """

            kwargs = {}
            if links is not None:
                kwargs["links"] = links
            if consistency_group_failover is not None:
                kwargs["consistency_group_failover"] = consistency_group_failover
            if create_destination is not None:
                kwargs["create_destination"] = create_destination
            if destination is not None:
                kwargs["destination"] = destination
            if exported_snapshot is not None:
                kwargs["exported_snapshot"] = exported_snapshot
            if group_type is not None:
                kwargs["group_type"] = group_type
            if healthy is not None:
                kwargs["healthy"] = healthy
            if identity_preservation is not None:
                kwargs["identity_preservation"] = identity_preservation
            if lag_time is not None:
                kwargs["lag_time"] = lag_time
            if last_transfer_network_compression_ratio is not None:
                kwargs["last_transfer_network_compression_ratio"] = last_transfer_network_compression_ratio
            if last_transfer_type is not None:
                kwargs["last_transfer_type"] = last_transfer_type
            if policy is not None:
                kwargs["policy"] = policy
            if preserve is not None:
                kwargs["preserve"] = preserve
            if quick_resync is not None:
                kwargs["quick_resync"] = quick_resync
            if recover_after_break is not None:
                kwargs["recover_after_break"] = recover_after_break
            if restore is not None:
                kwargs["restore"] = restore
            if restore_to_snapshot is not None:
                kwargs["restore_to_snapshot"] = restore_to_snapshot
            if source is not None:
                kwargs["source"] = source
            if state is not None:
                kwargs["state"] = state
            if svmdr_volumes is not None:
                kwargs["svmdr_volumes"] = svmdr_volumes
            if throttle is not None:
                kwargs["throttle"] = throttle
            if total_transfer_bytes is not None:
                kwargs["total_transfer_bytes"] = total_transfer_bytes
            if total_transfer_duration is not None:
                kwargs["total_transfer_duration"] = total_transfer_duration
            if transfer is not None:
                kwargs["transfer"] = transfer
            if transfer_schedule is not None:
                kwargs["transfer_schedule"] = transfer_schedule
            if unhealthy_reason is not None:
                kwargs["unhealthy_reason"] = unhealthy_reason
            if uuid is not None:
                kwargs["uuid"] = uuid

            resource = SnapmirrorRelationship(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create SnapmirrorRelationship: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a SnapMirror relationship. This API is used to initiate SnapMirror operations such as "initialize", "resync", "break", "quiesce", and "resume" by specifying the appropriate value for the "state" field. It is also used to modify the SnapMirror policy associated with the specified relationship. Additionally, a SnapMirror relationship can be failed over to the destination endpoint or a failed over SnapMirror relationship can be failed back to the original state or a SnapMirror relationship direction can be reversed using this API. This API can also be used to expand the SnapMirror Business Continuity relationship with the specified source and destination volumes.
<br>To initialize the relationship, PATCH the state to "snapmirrored" for relationships with a policy of type "async" or "in_sync" for relationships with a policy of type "sync".
<br>To break the relationship or to failover to the destination endpoint and start serving data from the destination endpoint, PATCH the state to "broken_off" for relationships with a policy of type "async" or "sync". SnapMirror relationships with the policy type as "sync" and sync_type as "automated_failover" cannot be "broken_off".
<br>To resync the broken relationship, PATCH the state to "snapmirrored" for relationships with a policy of type "async" or "in_sync" for relationships with a policy of type "sync".
<br>To failback the failed over relationship and start serving data from the source endpoint, PATCH the state to "snapmirrored" for relationships with a policy of type "async" or "in_sync" for relationships with a policy of type "sync" and set the query flag "failback" as "true". SnapMirror relationships with the policy type as "sync" and sync_type as "automated_failover" can be in "broken_off" state due to a failed attempt of automated SnapMirror failover operation.
<br>To pause the relationship, suspending further transfers, PATCH the state to "paused" for relationships with a policy of type "async" or "sync". SnapMirror relationships with the policy type as "sync" and sync_type as "automated_failover" cannot be "paused".
<br>To resume transfers for a paused relationship, PATCH the state to "snapmirrored" for relationships with a policy of type "async" or "in_sync" for relationships with a policy of type "sync".
<br>To reverse the direction of the relationship, PATCH the "source.path" with the destination endpoint and the "destination.path" with the source endpoint and the relationship state to "snapmirrored" for relationships with a policy of type "async" or "in_sync" for relationships with a policy of type "sync". For relationships with a policy of type "async" and relationship state as "snapmirrored", stop IO on the source endpoint and perform a SnapMirror transfer POST operation before reversing the direction of the relationship to prevent any loss of data.
<br>The values "in_sync", "out_of_sync", and "synchronizing" are only applicable to relationships with a policy of type "sync".
<br>When "transfer_schedule" is specified along with "state" during PATCH, first the schedule is modified on the relationship and then the respective SnapMirror operation is initiated. The "transfer_schedule" specified is used to update asynchronous relationships.
<br>When "throttle" is specified along with "state" during PATCH, first the throttle is modified on the relationship, which will be used by any upcoming transfers and then the respective SnapMirror operation is initiated. If the SnapMirror operation initiated a transfer then it will also use the new throttle. If "throttle" needs to be applied for a specific transfer use SnapMirror Transfer REST API.
<br>For SnapMirror Business Continuity (SMBC) relationships, when "consistency_group_volumes" is specified during PATCH, first the existing FlexVol volume relationship is deleted and released and then the volumes are added to the SMBC relationship.

### Examples
### Related ONTAP commands
* `snapmirror modify`
* `snapmirror initialize`
* `snapmirror resync`
* `snapmirror break`
* `snapmirror quiesce`
* `snapmirror resume`
### Important notes
* The property "transfer_schedule" if set on a SnapMirror relationship overrides the "transfer_schedule" set on the policy being used with the SnapMirror relationship.
* The property "throttle" if set on a SnapMirror relationship overrides the "throttle" set on the policy being used with the SnapMirror relationship.
* The properties "transfer_schedule" and "throttle" are not supported when "failback" is set to "true".
* The properties "transfer_schedule" and "throttle" are not supported when "failover" is set to "true".
* The properties "transfer_schedule" and "throttle" are not supported when "force_failover" is set to "true".
* The properties "transfer_schedule" and "throttle" are not supported when the direction of the relationship is being reversed.
* To remove a transfer_schedule on a SnapMirror relationship set the "transfer_schedule" to null (no-quotes) during SnapMirror relationship PATCH.
* The property "identity_preservation" value can be changed from a higher "identity_preservation" threshold value to a lower "identity_preservation" threshold value but not vice-versa. For example, the threshold value of the "identity_preservation" property can be changed from "full" to "exclude_network_config", but cannot be increased from "exclude_network_and_protocol_config" to "exclude_network_config" to "full". The threshold value of the "identity_preservation" cannot be changed to "exclude_network_and_protocol_config" for IDP SVMDR.

### Examples
The following examples show how to perform the SnapMirror "resync", "initialize", "resume", "quiesce", and "break" operations. In addition, a relationship can be failed over to the destination endpoint and start serving data from the destination endpoint. A failed over relationship can be failed back to the source endpoint and serve data from the source endpoint. Also a relationship can be reversed by making the source endpoint as the new destination endpoint and the destination endpoint as the new source endpoint.
<br/>
   To update an associated SnapMirror policy.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"policy": { "name" : "MirrorAndVaultDiscardNetwork"}}'
   ```
   <br/>
   To perform SnapMirror "resync" for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored"}'
   ```
   <br/>
   To perform SnapMirror "initialize" for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored"}'
   ```
   <br/>
   To perform SnapMirror "resume" for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored"}'
   ```
   <br/>
   To perform SnapMirror "quiesce" for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff" '{"state":"paused"}'
   ```
   <br/>
   To perform SnapMirror "break" for an asynchronous SnapMirror relationship. This operation does a failover to the destination endpoint. After a the failover, data can then be served from the destination endpoint.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff" '{"state":"broken_off"}'
   ```
   <br/>
   To forcefully failover to the destination endpoint and start serving data from the destination endpoint.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/?force=true" '{"state":"broken_off"}'
   ```
   <br/>
   To failback to the source endpoint and start serving data from the source endpoint for an asynchronous relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/?failback=true" '{"state":"snapmirrored"}'
   ```
   <br/>
   To failback to the source endpoint and start serving data from the source endpoint for a synchronous relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/?failback=true" '{"state":"in_sync"}'
   ```
   <br/>
   To reverse the direction of an asynchronous relationship, that is, make the source endpoint as the new destination endpoint and make the destination endpoint as the new source endpoint.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"source": {"path": "dst_svm:dst_vol"}, "destination": {"path": "src_svm:src_vol"}, "state": "snapmirrored"}'
   ```
   <br/>
   To reverse the direction of a synchronous relationship, that is, make the source endpoint as the new destination endpoint and make the destination endpoint as the new source endpoint.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"source": {"path": "dst_svm:dst_vol"}, "destination": {"path": "src_svm:src_vol"}, "state": "in_sync"}'
   ```
   <br/>
   Updating SnapMirror transfer_schedule and throttle for an asynchronous SnapMirror relationship. Transfer_schedule can be specified as UUID or name or both.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"transfer_schedule":{"uuid":"817500fa-092d-44c5-9c10-7b54f7b2f20a", "name":"5min"}, "throttle":100}'
   ```
   <br/>
   Removing the SnapMirror transfer_schedule for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"transfer_schedule":{"uuid":null, "name":null}}'
   ```
   <br/>
   Removing the SnapMirror throttle for an asynchronous SnapMirror relationship.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"throttle":0}'
   ```
   <br/>
   To perform SnapMirror "resync" and update the SnapMirror transfer_schedule for an asynchronous SnapMirror relationship. First the transfer_schedule is modified and then the resync transfer is initiated.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored", transfer_schedule":{"uuid":"817500fa-092d-44c5-9c10-7b54f7b2f20a", "name":"5min"}}'
   ```
   <br/>
   To perform SnapMirror "initialize" and update the SnapMirror throttle for an asynchronous SnapMirror relationship. First the throttle is modified and then the initialize transfer is initiated. The initialize transfer will use this new throttle.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored", "throttle":100}'
   ```
   <br/>
   To perform SnapMirror "resync" and update the SnapMirror throttle for an asynchronous SnapMirror relationship. First the throttle is modified and then the resync transfer is initiated. The resync transfer will use this new throttle.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{"state":"snapmirrored", "throttle":100}'
   ```
   <br/>
   To perform a SnapMirror Business Continuity expansion.
   <br/>
   ```
   PATCH "/api/snapmirror/relationships/98bb2608-fc60-11e8-aa13-005056a707ff/" '{ "source" : {"consistency_group_volumes":[{"name":"vol"}]}, "destination" : {"consistency_group_volumes":[{"name":"voldp"}]} }'
### Learn more
* [`DOC /snapmirror/relationships`](#docs-snapmirror-snapmirror_relationships)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="snapmirror relationship modify")
        async def snapmirror_relationship_modify(
        ) -> ResourceTable:
            """Modify an instance of a SnapmirrorRelationship resource

            Args:
                exported_snapshot: Snapshot copy exported to clients on destination.
                query_exported_snapshot: Snapshot copy exported to clients on destination.
                group_type: Specifies the group type of the top level SnapMirror relationship. The volume relationships are shown as _none_, the SVMDR relationships are shown as _svm_dr_, the Consistency Group relationships are shown as _consistency_group_, and the FlexGroup volume relationships are shown as _flexgroup_.
                query_group_type: Specifies the group type of the top level SnapMirror relationship. The volume relationships are shown as _none_, the SVMDR relationships are shown as _svm_dr_, the Consistency Group relationships are shown as _consistency_group_, and the FlexGroup volume relationships are shown as _flexgroup_.
                healthy: Is the relationship healthy?
                query_healthy: Is the relationship healthy?
                identity_preservation: Specifies which configuration of the source SVM is replicated to the destination SVM. This property is applicable only for SVM data protection with \"async\" policy type. This \"identity_preservation\" overrides the \"identity_preservation\" set on the SnapMirror relationship's policy.
                query_identity_preservation: Specifies which configuration of the source SVM is replicated to the destination SVM. This property is applicable only for SVM data protection with \"async\" policy type. This \"identity_preservation\" overrides the \"identity_preservation\" set on the SnapMirror relationship's policy.
                lag_time: Time since the exported Snapshot copy was created.
                query_lag_time: Time since the exported Snapshot copy was created.
                last_transfer_network_compression_ratio: Specifies the compression ratio achieved for the data sent over the wire with network compression enabled for the last successful transfer.
                query_last_transfer_network_compression_ratio: Specifies the compression ratio achieved for the data sent over the wire with network compression enabled for the last successful transfer.
                last_transfer_type: Specifies the operation type of the last transfer that occurred on the relationship. The _initialize_ transfer occurs when the relationship state changes from uninitialized to snapmirrored or in_sync. The _update_ transfer occurs when the snapshots are transferred from the source endpoint to the destination endpoint as part of scheduled or manual update. The _resync_ transfer occurs when the relationship state changes from broken_off to snapmirrored or in_sync. The _restore_ transfer occurs when the snapshot is restored from a destination endpoint to another endpoint.
                query_last_transfer_type: Specifies the operation type of the last transfer that occurred on the relationship. The _initialize_ transfer occurs when the relationship state changes from uninitialized to snapmirrored or in_sync. The _update_ transfer occurs when the snapshots are transferred from the source endpoint to the destination endpoint as part of scheduled or manual update. The _resync_ transfer occurs when the relationship state changes from broken_off to snapmirrored or in_sync. The _restore_ transfer occurs when the snapshot is restored from a destination endpoint to another endpoint.
                preserve: Set to true on resync to preserve Snapshot copies on the destination that are newer than the latest common Snapshot copy. This property is applicable only for relationships with FlexVol volume or FlexGroup volume endpoints and when the PATCH state is being changed to \"snapmirrored\".
                query_preserve: Set to true on resync to preserve Snapshot copies on the destination that are newer than the latest common Snapshot copy. This property is applicable only for relationships with FlexVol volume or FlexGroup volume endpoints and when the PATCH state is being changed to \"snapmirrored\".
                quick_resync: Set to true to reduce resync time by not preserving storage efficiency. This property is applicable only for relationships with FlexVol volume endpoints and SVMDR relationships when the PATCH state is being changed to \"snapmirrored\".
                query_quick_resync: Set to true to reduce resync time by not preserving storage efficiency. This property is applicable only for relationships with FlexVol volume endpoints and SVMDR relationships when the PATCH state is being changed to \"snapmirrored\".
                recover_after_break: Set to true to recover from a failed SnapMirror break operation on a FlexGroup volume relationship. This restores all destination FlexGroup constituent volumes to the latest Snapshot copy, and any writes to the read-write constituents are lost. This property is applicable only for SnapMirror relationships with FlexGroup volume endpoints and when the PATCH state is being changed to \"broken_off\".
                query_recover_after_break: Set to true to recover from a failed SnapMirror break operation on a FlexGroup volume relationship. This restores all destination FlexGroup constituent volumes to the latest Snapshot copy, and any writes to the read-write constituents are lost. This property is applicable only for SnapMirror relationships with FlexGroup volume endpoints and when the PATCH state is being changed to \"broken_off\".
                restore: Set to true to create a relationship for restore. To trigger restore-transfer, use transfers POST on the restore relationship. SnapMirror relationships with the policy type \"async\" can be restored. SnapMirror relationships with the policy type \"sync\" cannot be restored.
                query_restore: Set to true to create a relationship for restore. To trigger restore-transfer, use transfers POST on the restore relationship. SnapMirror relationships with the policy type \"async\" can be restored. SnapMirror relationships with the policy type \"sync\" cannot be restored.
                restore_to_snapshot: Specifies the Snapshot copy to restore to on the destination during the break operation. This property is applicable only for SnapMirror relationships with FlexVol volume endpoints and when the PATCH state is being changed to \"broken_off\".
                query_restore_to_snapshot: Specifies the Snapshot copy to restore to on the destination during the break operation. This property is applicable only for SnapMirror relationships with FlexVol volume endpoints and when the PATCH state is being changed to \"broken_off\".
                state: State of the relationship.<br>To initialize the relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".<br>To break the relationship, PATCH the state to \"broken_off\" for relationships with a policy of type \"async\" or \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" cannot be \"broken_off\".<br>To resync the relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" can be in \"broken_off\" state due to a failed attempt of SnapMirror failover.<br>To pause the relationship, suspending further transfers, PATCH the state to \"paused\" for relationships with a policy of type \"async\" or \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" cannot be \"paused\".<br>To resume transfers for a paused relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".<br>The entries \"in_sync\", \"out_of_sync\", \"synchronizing\", and \"expanding\" are only applicable to relationships with a policy of type \"sync\". A PATCH call on the state change only triggers the transition to the specified state. You must poll on the \"state\", \"healthy\" and \"unhealthy_reason\" properties using a GET request to determine if the transition is successful. To automatically initialize the relationship when specifying \"create_destination\" property, set the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".
                query_state: State of the relationship.<br>To initialize the relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".<br>To break the relationship, PATCH the state to \"broken_off\" for relationships with a policy of type \"async\" or \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" cannot be \"broken_off\".<br>To resync the relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" can be in \"broken_off\" state due to a failed attempt of SnapMirror failover.<br>To pause the relationship, suspending further transfers, PATCH the state to \"paused\" for relationships with a policy of type \"async\" or \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" cannot be \"paused\".<br>To resume transfers for a paused relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".<br>The entries \"in_sync\", \"out_of_sync\", \"synchronizing\", and \"expanding\" are only applicable to relationships with a policy of type \"sync\". A PATCH call on the state change only triggers the transition to the specified state. You must poll on the \"state\", \"healthy\" and \"unhealthy_reason\" properties using a GET request to determine if the transition is successful. To automatically initialize the relationship when specifying \"create_destination\" property, set the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".
                throttle: Throttle, in KBs per second. This \"throttle\" overrides the \"throttle\" set on the SnapMirror relationship's policy. If neither of these are set, defaults to 0, which is interpreted as unlimited.
                query_throttle: Throttle, in KBs per second. This \"throttle\" overrides the \"throttle\" set on the SnapMirror relationship's policy. If neither of these are set, defaults to 0, which is interpreted as unlimited.
                total_transfer_bytes: Indicates the cumulative bytes transfered since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.
                query_total_transfer_bytes: Indicates the cumulative bytes transfered since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.
                total_transfer_duration: Indicates the cumulative duration of all transfers since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.
                query_total_transfer_duration: Indicates the cumulative duration of all transfers since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.
                uuid: 
                query_uuid: 
            """

            kwargs = {}
            changes = {}
            if query_exported_snapshot is not None:
                kwargs["exported_snapshot"] = query_exported_snapshot
            if query_group_type is not None:
                kwargs["group_type"] = query_group_type
            if query_healthy is not None:
                kwargs["healthy"] = query_healthy
            if query_identity_preservation is not None:
                kwargs["identity_preservation"] = query_identity_preservation
            if query_lag_time is not None:
                kwargs["lag_time"] = query_lag_time
            if query_last_transfer_network_compression_ratio is not None:
                kwargs["last_transfer_network_compression_ratio"] = query_last_transfer_network_compression_ratio
            if query_last_transfer_type is not None:
                kwargs["last_transfer_type"] = query_last_transfer_type
            if query_preserve is not None:
                kwargs["preserve"] = query_preserve
            if query_quick_resync is not None:
                kwargs["quick_resync"] = query_quick_resync
            if query_recover_after_break is not None:
                kwargs["recover_after_break"] = query_recover_after_break
            if query_restore is not None:
                kwargs["restore"] = query_restore
            if query_restore_to_snapshot is not None:
                kwargs["restore_to_snapshot"] = query_restore_to_snapshot
            if query_state is not None:
                kwargs["state"] = query_state
            if query_throttle is not None:
                kwargs["throttle"] = query_throttle
            if query_total_transfer_bytes is not None:
                kwargs["total_transfer_bytes"] = query_total_transfer_bytes
            if query_total_transfer_duration is not None:
                kwargs["total_transfer_duration"] = query_total_transfer_duration
            if query_uuid is not None:
                kwargs["uuid"] = query_uuid

            if exported_snapshot is not None:
                changes["exported_snapshot"] = exported_snapshot
            if group_type is not None:
                changes["group_type"] = group_type
            if healthy is not None:
                changes["healthy"] = healthy
            if identity_preservation is not None:
                changes["identity_preservation"] = identity_preservation
            if lag_time is not None:
                changes["lag_time"] = lag_time
            if last_transfer_network_compression_ratio is not None:
                changes["last_transfer_network_compression_ratio"] = last_transfer_network_compression_ratio
            if last_transfer_type is not None:
                changes["last_transfer_type"] = last_transfer_type
            if preserve is not None:
                changes["preserve"] = preserve
            if quick_resync is not None:
                changes["quick_resync"] = quick_resync
            if recover_after_break is not None:
                changes["recover_after_break"] = recover_after_break
            if restore is not None:
                changes["restore"] = restore
            if restore_to_snapshot is not None:
                changes["restore_to_snapshot"] = restore_to_snapshot
            if state is not None:
                changes["state"] = state
            if throttle is not None:
                changes["throttle"] = throttle
            if total_transfer_bytes is not None:
                changes["total_transfer_bytes"] = total_transfer_bytes
            if total_transfer_duration is not None:
                changes["total_transfer_duration"] = total_transfer_duration
            if uuid is not None:
                changes["uuid"] = uuid

            if hasattr(SnapmirrorRelationship, "find"):
                resource = SnapmirrorRelationship.find(
                    **kwargs
                )
            else:
                resource = SnapmirrorRelationship()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify SnapmirrorRelationship: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a SnapMirror relationship.
### Important notes
* The "destination_only", "source_only", and "source_info_only" flags are mutually exclusive. If no flag is specified, the relationship is deleted from both the source and destination and all common Snapshot copies between the source and destination are also deleted.
* For a restore relationship, the call must be executed on the cluster containing the destination endpoint without specifying the destination_only, source_only, or source_info_only parameters.
* Additionally, ensure that there are no ongoing transfers on a restore relationship before calling this API.
* The "failover", "force-failover" and "failback" query parameters are only applicable for SVM-DR SnapMirror relationships.
* When a SnapMirror relationship associated with a pair of source and destination Consistency Groups is deleted, the corresponding Consistency Groups on the source and destination clusters are not automatically deleted and remain in place.
### Related ONTAP commands
* `snapmirror delete`
* `snapmirror release`
### Examples
The following examples show how to delete the relationship from both the source and destination, the destination only, and the source only.
<br/>
   Deleting the relationship from both the source and destination. This API must be run on the cluster containing the destination endpoint.
   <br/>
   ```
   DELETE "/api/snapmirror/relationships/4512b2d2-fd60-11e8-8929-005056bbfe52"
   ```
   <br/>
   Deleting the relationship on the destination only. This API must be run on the cluster containing the destination endpoint.
   <br/>
   ```
   DELETE "/api/snapmirror/relationships/fd1e0697-02ba-11e9-acc7-005056a7697f/?destination_only=true"
   ```
   <br/>
   Deleting the relationship on the source only. This API must be run on the cluster containing the source endpoint.
   <br/>
   ```
   DELETE "/api/snapmirror/relationships/93e828ba-02bc-11e9-acc7-005056a7697f/?source_only=true"
   ```
   <br/>
   Deleting the source information only. This API must be run on the cluster containing the source endpoint. This does not delete the common Snapshot copies between the source and destination.
   <br/>
   ```
   DELETE "/api/snapmirror/relationships/caf545a2-fc60-11e8-aa13-005056a707ff/?source_info_only=true"
   ```
   <br/>
### Learn more
* [`DOC /snapmirror/relationships`](#docs-snapmirror-snapmirror_relationships)
"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="snapmirror relationship delete")
        async def snapmirror_relationship_delete(
        ) -> None:
            """Delete an instance of a SnapmirrorRelationship resource

            Args:
                exported_snapshot: Snapshot copy exported to clients on destination.
                group_type: Specifies the group type of the top level SnapMirror relationship. The volume relationships are shown as _none_, the SVMDR relationships are shown as _svm_dr_, the Consistency Group relationships are shown as _consistency_group_, and the FlexGroup volume relationships are shown as _flexgroup_.
                healthy: Is the relationship healthy?
                identity_preservation: Specifies which configuration of the source SVM is replicated to the destination SVM. This property is applicable only for SVM data protection with \"async\" policy type. This \"identity_preservation\" overrides the \"identity_preservation\" set on the SnapMirror relationship's policy.
                lag_time: Time since the exported Snapshot copy was created.
                last_transfer_network_compression_ratio: Specifies the compression ratio achieved for the data sent over the wire with network compression enabled for the last successful transfer.
                last_transfer_type: Specifies the operation type of the last transfer that occurred on the relationship. The _initialize_ transfer occurs when the relationship state changes from uninitialized to snapmirrored or in_sync. The _update_ transfer occurs when the snapshots are transferred from the source endpoint to the destination endpoint as part of scheduled or manual update. The _resync_ transfer occurs when the relationship state changes from broken_off to snapmirrored or in_sync. The _restore_ transfer occurs when the snapshot is restored from a destination endpoint to another endpoint.
                preserve: Set to true on resync to preserve Snapshot copies on the destination that are newer than the latest common Snapshot copy. This property is applicable only for relationships with FlexVol volume or FlexGroup volume endpoints and when the PATCH state is being changed to \"snapmirrored\".
                quick_resync: Set to true to reduce resync time by not preserving storage efficiency. This property is applicable only for relationships with FlexVol volume endpoints and SVMDR relationships when the PATCH state is being changed to \"snapmirrored\".
                recover_after_break: Set to true to recover from a failed SnapMirror break operation on a FlexGroup volume relationship. This restores all destination FlexGroup constituent volumes to the latest Snapshot copy, and any writes to the read-write constituents are lost. This property is applicable only for SnapMirror relationships with FlexGroup volume endpoints and when the PATCH state is being changed to \"broken_off\".
                restore: Set to true to create a relationship for restore. To trigger restore-transfer, use transfers POST on the restore relationship. SnapMirror relationships with the policy type \"async\" can be restored. SnapMirror relationships with the policy type \"sync\" cannot be restored.
                restore_to_snapshot: Specifies the Snapshot copy to restore to on the destination during the break operation. This property is applicable only for SnapMirror relationships with FlexVol volume endpoints and when the PATCH state is being changed to \"broken_off\".
                state: State of the relationship.<br>To initialize the relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".<br>To break the relationship, PATCH the state to \"broken_off\" for relationships with a policy of type \"async\" or \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" cannot be \"broken_off\".<br>To resync the relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" can be in \"broken_off\" state due to a failed attempt of SnapMirror failover.<br>To pause the relationship, suspending further transfers, PATCH the state to \"paused\" for relationships with a policy of type \"async\" or \"sync\". SnapMirror relationships with the policy type as \"sync\" and \"sync_type\" as \"automated_failover\" cannot be \"paused\".<br>To resume transfers for a paused relationship, PATCH the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".<br>The entries \"in_sync\", \"out_of_sync\", \"synchronizing\", and \"expanding\" are only applicable to relationships with a policy of type \"sync\". A PATCH call on the state change only triggers the transition to the specified state. You must poll on the \"state\", \"healthy\" and \"unhealthy_reason\" properties using a GET request to determine if the transition is successful. To automatically initialize the relationship when specifying \"create_destination\" property, set the state to \"snapmirrored\" for relationships with a policy of type \"async\" or to state \"in_sync\" for relationships with a policy of type \"sync\".
                throttle: Throttle, in KBs per second. This \"throttle\" overrides the \"throttle\" set on the SnapMirror relationship's policy. If neither of these are set, defaults to 0, which is interpreted as unlimited.
                total_transfer_bytes: Indicates the cumulative bytes transfered since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.
                total_transfer_duration: Indicates the cumulative duration of all transfers since the last aggregate relocation, takeover/giveback, or metrocluster switchover/switchback involving the node that hosts the relationship.
                uuid: 
            """

            kwargs = {}
            if exported_snapshot is not None:
                kwargs["exported_snapshot"] = exported_snapshot
            if group_type is not None:
                kwargs["group_type"] = group_type
            if healthy is not None:
                kwargs["healthy"] = healthy
            if identity_preservation is not None:
                kwargs["identity_preservation"] = identity_preservation
            if lag_time is not None:
                kwargs["lag_time"] = lag_time
            if last_transfer_network_compression_ratio is not None:
                kwargs["last_transfer_network_compression_ratio"] = last_transfer_network_compression_ratio
            if last_transfer_type is not None:
                kwargs["last_transfer_type"] = last_transfer_type
            if preserve is not None:
                kwargs["preserve"] = preserve
            if quick_resync is not None:
                kwargs["quick_resync"] = quick_resync
            if recover_after_break is not None:
                kwargs["recover_after_break"] = recover_after_break
            if restore is not None:
                kwargs["restore"] = restore
            if restore_to_snapshot is not None:
                kwargs["restore_to_snapshot"] = restore_to_snapshot
            if state is not None:
                kwargs["state"] = state
            if throttle is not None:
                kwargs["throttle"] = throttle
            if total_transfer_bytes is not None:
                kwargs["total_transfer_bytes"] = total_transfer_bytes
            if total_transfer_duration is not None:
                kwargs["total_transfer_duration"] = total_transfer_duration
            if uuid is not None:
                kwargs["uuid"] = uuid

            if hasattr(SnapmirrorRelationship, "find"):
                resource = SnapmirrorRelationship.find(
                    **kwargs
                )
            else:
                resource = SnapmirrorRelationship()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete SnapmirrorRelationship: %s" % err)


