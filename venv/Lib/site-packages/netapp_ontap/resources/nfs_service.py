r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

### Retrieving an NFS configuration
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import NfsService

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(NfsService.get_collection()))

```

### Retrieving the mount permissions for a specified volume for a given IP address.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import NfsService

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(
        list(
            NfsService.get_collection(
                fields="*",
                return_timeout=15,
                **{
                    "protocol_access_rules.volume": "testvol12",
                    "protocol_access_rules.client_ip": "1.2.3.4",
                    "protocol_access_rules.auth_type": "sys",
                    "svm.uuid": "525928e9-9f84-11eb-a89f-005056bb70a8",
                }
            )
        )
    )

```

### Creating an NFS configuration for an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import NfsService

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = NfsService()
    resource.svm = {"uuid": "1cd8a442-86d1-11e0-ae1c-123478563412"}
    resource.protocol = {"v4_id_domain": "nfs-nsr-w01.rtp.netapp.com"}
    resource.vstorage_enabled = True
    resource.post(hydrate=True)
    print(resource)

```

### Updating an  NFS configuration for an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import NfsService

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = NfsService(**{"svm.uuid": "4a415601-548c-11e8-a21d-0050568bcbc9"})
    resource.protocol = {"v4_id_domain": "nfs-nsr-w01.rtp.netapp.com"}
    resource.vstorage_enabled = False
    resource.patch()

```

### Deleting an NFS configuration for an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import NfsService

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = NfsService(**{"svm.uuid": "4a415601-548c-11e8-a21d-0050568bcbc9"})
    resource.delete()

```

## Performance monitoring
Performance of the SVM can be monitored by the `metric.*` and `statistics.*` properties. These show the performance of the SVM in terms of IOPS, latency and throughput. The `metric.*` properties denote an average whereas `statistics.*` properties denote a real-time monotonically increasing value aggregated across all nodes."""

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


__all__ = ["NfsService", "NfsServiceSchema"]
__pdoc__ = {
    "NfsServiceSchema.resource": False,
    "NfsServiceSchema.opts": False,
    "NfsService.nfs_service_show": False,
    "NfsService.nfs_service_create": False,
    "NfsService.nfs_service_modify": False,
    "NfsService.nfs_service_delete": False,
}


class NfsServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NfsService object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the nfs_service."""

    access_cache_config = fields.Nested("netapp_ontap.models.nfs_service_access_cache_config.NfsServiceAccessCacheConfigSchema", data_key="access_cache_config", unknown=EXCLUDE)
    r""" The access_cache_config field of the nfs_service."""

    auth_sys_extended_groups_enabled = fields.Boolean(
        data_key="auth_sys_extended_groups_enabled",
    )
    r""" Specifies whether or not extended groups support over AUTH_SYS is enabled."""

    credential_cache = fields.Nested("netapp_ontap.models.nfs_service_credential_cache.NfsServiceCredentialCacheSchema", data_key="credential_cache", unknown=EXCLUDE)
    r""" The credential_cache field of the nfs_service."""

    enabled = fields.Boolean(
        data_key="enabled",
    )
    r""" Specifies if the NFS service is administratively enabled."""

    exports = fields.Nested("netapp_ontap.models.nfs_service_exports.NfsServiceExportsSchema", data_key="exports", unknown=EXCLUDE)
    r""" The exports field of the nfs_service."""

    extended_groups_limit = Size(
        data_key="extended_groups_limit",
        validate=integer_validation(minimum=32, maximum=1024),
    )
    r""" Specifies the maximum auxiliary groups supported over AUTH_SYS and RPCSEC_GSS.

Example: 32"""

    file_session_io_grouping_count = Size(
        data_key="file_session_io_grouping_count",
        validate=integer_validation(minimum=1000, maximum=20000),
    )
    r""" Number of I/O operations on a file to be grouped and considered as one session for event generation applications, such as FPolicy.

Example: 5000"""

    file_session_io_grouping_duration = Size(
        data_key="file_session_io_grouping_duration",
        validate=integer_validation(minimum=60, maximum=600),
    )
    r""" The duration for which I/O operations on a file will be grouped and considered as one session for event generation applications, such as FPolicy.

Example: 120"""

    metric = fields.Nested("netapp_ontap.models.performance_svm_nfs_metric.PerformanceSvmNfsMetricSchema", data_key="metric", unknown=EXCLUDE)
    r""" The metric field of the nfs_service."""

    protocol = fields.Nested("netapp_ontap.models.nfs_service_protocol.NfsServiceProtocolSchema", data_key="protocol", unknown=EXCLUDE)
    r""" The protocol field of the nfs_service."""

    protocol_access_rules = fields.Nested("netapp_ontap.models.nfs_service_protocol_access_rules.NfsServiceProtocolAccessRulesSchema", data_key="protocol_access_rules", unknown=EXCLUDE)
    r""" The protocol_access_rules field of the nfs_service."""

    qtree = fields.Nested("netapp_ontap.models.nfs_service_qtree.NfsServiceQtreeSchema", data_key="qtree", unknown=EXCLUDE)
    r""" The qtree field of the nfs_service."""

    root = fields.Nested("netapp_ontap.models.nfs_service_root.NfsServiceRootSchema", data_key="root", unknown=EXCLUDE)
    r""" The root field of the nfs_service."""

    rquota_enabled = fields.Boolean(
        data_key="rquota_enabled",
    )
    r""" Specifies whether or not the remote quota feature is enabled."""

    security = fields.Nested("netapp_ontap.models.nfs_service_security.NfsServiceSecuritySchema", data_key="security", unknown=EXCLUDE)
    r""" The security field of the nfs_service."""

    showmount_enabled = fields.Boolean(
        data_key="showmount_enabled",
    )
    r""" Specifies whether or not the showmount feature is enabled."""

    state = fields.Str(
        data_key="state",
        validate=enum_validation(['online', 'offline']),
    )
    r""" Specifies the state of the NFS service on the SVM. The following values are supported:

          * online - NFS server is ready to accept client requests.
          * offline - NFS server is not ready to accept client requests.


Valid choices:

* online
* offline"""

    statistics = fields.Nested("netapp_ontap.models.performance_svm_nfs_statistics.PerformanceSvmNfsStatisticsSchema", data_key="statistics", unknown=EXCLUDE)
    r""" The statistics field of the nfs_service."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the nfs_service."""

    transport = fields.Nested("netapp_ontap.models.nfs_service_transport.NfsServiceTransportSchema", data_key="transport", unknown=EXCLUDE)
    r""" The transport field of the nfs_service."""

    vstorage_enabled = fields.Boolean(
        data_key="vstorage_enabled",
    )
    r""" Specifies whether or not the VMware vstorage feature is enabled."""

    windows = fields.Nested("netapp_ontap.models.nfs_service_windows.NfsServiceWindowsSchema", data_key="windows", unknown=EXCLUDE)
    r""" The windows field of the nfs_service."""

    @property
    def resource(self):
        return NfsService

    gettable_fields = [
        "links",
        "access_cache_config",
        "auth_sys_extended_groups_enabled",
        "credential_cache",
        "enabled",
        "exports",
        "extended_groups_limit",
        "file_session_io_grouping_count",
        "file_session_io_grouping_duration",
        "metric",
        "protocol",
        "protocol_access_rules",
        "qtree",
        "root",
        "rquota_enabled",
        "security",
        "showmount_enabled",
        "state",
        "statistics",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "transport",
        "vstorage_enabled",
        "windows",
    ]
    """links,access_cache_config,auth_sys_extended_groups_enabled,credential_cache,enabled,exports,extended_groups_limit,file_session_io_grouping_count,file_session_io_grouping_duration,metric,protocol,protocol_access_rules,qtree,root,rquota_enabled,security,showmount_enabled,state,statistics,svm.links,svm.name,svm.uuid,transport,vstorage_enabled,windows,"""

    patchable_fields = [
        "access_cache_config",
        "auth_sys_extended_groups_enabled",
        "credential_cache",
        "enabled",
        "exports",
        "extended_groups_limit",
        "file_session_io_grouping_count",
        "file_session_io_grouping_duration",
        "protocol",
        "protocol_access_rules",
        "qtree",
        "root",
        "rquota_enabled",
        "security",
        "showmount_enabled",
        "svm.name",
        "svm.uuid",
        "transport",
        "vstorage_enabled",
        "windows",
    ]
    """access_cache_config,auth_sys_extended_groups_enabled,credential_cache,enabled,exports,extended_groups_limit,file_session_io_grouping_count,file_session_io_grouping_duration,protocol,protocol_access_rules,qtree,root,rquota_enabled,security,showmount_enabled,svm.name,svm.uuid,transport,vstorage_enabled,windows,"""

    postable_fields = [
        "access_cache_config",
        "auth_sys_extended_groups_enabled",
        "credential_cache",
        "enabled",
        "exports",
        "extended_groups_limit",
        "file_session_io_grouping_count",
        "file_session_io_grouping_duration",
        "protocol",
        "protocol_access_rules",
        "qtree",
        "root",
        "rquota_enabled",
        "security",
        "showmount_enabled",
        "svm.name",
        "svm.uuid",
        "transport",
        "vstorage_enabled",
        "windows",
    ]
    """access_cache_config,auth_sys_extended_groups_enabled,credential_cache,enabled,exports,extended_groups_limit,file_session_io_grouping_count,file_session_io_grouping_duration,protocol,protocol_access_rules,qtree,root,rquota_enabled,security,showmount_enabled,svm.name,svm.uuid,transport,vstorage_enabled,windows,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in NfsService.get_collection(fields=field)]
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
            raise NetAppRestError("NfsService modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class NfsService(Resource):
    """Allows interaction with NfsService objects on the host"""

    _schema = NfsServiceSchema
    _path = "/api/protocols/nfs/services"
    _keys = ["svm.uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the NFS configuration of SVMs.
### Expensive properties
There is an added computational cost to retrieving values for these properties. They are not included by default in GET results and must be explicitly requested using the `fields` query parameter. See [`Requesting specific fields`](#Requesting_specific_fields) to learn more.
* `statistics.*`
* `metric.*`
### Advanced properties
* `security.rpcsec_context_idle`
* `security.ntfs_unix_security`
* `security.chown_mode`
* `security.nt_acl_display_permission`
* `protocol.v3_features.ejukebox_enabled`
* `protocol.v3_features.connection_drop`
* `protocol.v3_features.fsid_change`
* `protocol.v3_features.mount_daemon_port`
* `protocol.v3_features.network_lock_manager_port`
* `protocol.v3_features.network_status_monitor_port`
* `protocol.v3_features.rquota_daemon_port`
* `protocol.v3_features.hide_snapshot_enabled`
* `protocol.v41_features.implementation_domain`
* `protocol.v41_features.implementation_name`
* `protocol.v40_features.acl_max_aces`
* `windows.map_unknown_uid_to_default_user`
* `exports.netgroup_trust_any_nsswitch_no_match`
* `credential_cache.negative_ttl`
* `transport.tcp_max_transfer_size`
* `root.*`
* `protocol.v41_features.trunking_enabled`
* `protocol.v42_features.seclabel_enabled`
* `protocol.v42_features.sparsefile_ops_enabled`
* `protocol.v42_features.xattrs_enabled`
* `protocol.v40_features.referrals_enabled`
* `protocol.v41_features.referrals_enabled`
* `protocol.v4_lease_seconds`
* `protocol.v4_session_slots`
* `protocol.v4_session_slot_reply_cache_size`
* `protocol.v4_fsid_change`
### Diagnostic properties
* `credential_cache.transient_error_ttl`
* `access_cache_config.ttl_failure`
### Related ONTAP commands
* `vserver nfs show`
* `vserver nfs status`
### Learn more
* [`DOC /protocols/nfs/services`](#docs-NAS-protocols_nfs_services)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="nfs service show")
        def nfs_service_show(
            fields: List[Choices.define(["auth_sys_extended_groups_enabled", "enabled", "extended_groups_limit", "file_session_io_grouping_count", "file_session_io_grouping_duration", "rquota_enabled", "showmount_enabled", "state", "vstorage_enabled", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of NfsService resources

            Args:
                auth_sys_extended_groups_enabled: Specifies whether or not extended groups support over AUTH_SYS is enabled.
                enabled: Specifies if the NFS service is administratively enabled. 
                extended_groups_limit: Specifies the maximum auxiliary groups supported over AUTH_SYS and RPCSEC_GSS.
                file_session_io_grouping_count: Number of I/O operations on a file to be grouped and considered as one session for event generation applications, such as FPolicy.
                file_session_io_grouping_duration: The duration for which I/O operations on a file will be grouped and considered as one session for event generation applications, such as FPolicy.
                rquota_enabled: Specifies whether or not the remote quota feature is enabled.
                showmount_enabled: Specifies whether or not the showmount feature is enabled.
                state: Specifies the state of the NFS service on the SVM. The following values are supported:           * online - NFS server is ready to accept client requests.           * offline - NFS server is not ready to accept client requests. 
                vstorage_enabled: Specifies whether or not the VMware vstorage feature is enabled.
            """

            kwargs = {}
            if auth_sys_extended_groups_enabled is not None:
                kwargs["auth_sys_extended_groups_enabled"] = auth_sys_extended_groups_enabled
            if enabled is not None:
                kwargs["enabled"] = enabled
            if extended_groups_limit is not None:
                kwargs["extended_groups_limit"] = extended_groups_limit
            if file_session_io_grouping_count is not None:
                kwargs["file_session_io_grouping_count"] = file_session_io_grouping_count
            if file_session_io_grouping_duration is not None:
                kwargs["file_session_io_grouping_duration"] = file_session_io_grouping_duration
            if rquota_enabled is not None:
                kwargs["rquota_enabled"] = rquota_enabled
            if showmount_enabled is not None:
                kwargs["showmount_enabled"] = showmount_enabled
            if state is not None:
                kwargs["state"] = state
            if vstorage_enabled is not None:
                kwargs["vstorage_enabled"] = vstorage_enabled
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return NfsService.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all NfsService resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["NfsService"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates the NFS configuration of an SVM.
### Related ONTAP commands
* `vserver nfs modify`
* `vserver nfs on`
* `vserver nfs off`
* `vserver nfs start`
* `vserver nfs stop`
### Learn more
* [`DOC /protocols/nfs/services`](#docs-NAS-protocols_nfs_services)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["NfsService"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["NfsService"], NetAppResponse]:
        r"""Creates an NFS configuration for an SVM.
### Required properties
* `svm.uuid` or `svm.name` - Existing SVM for which to create the NFS configuration.
### Default property values
If not specified in POST, the following default property values are assigned:
* `enabled` - _true_
* `state` - online
* `transport.udp_enabled` - _true_
* `transport.tcp_enabled` - _true_
* `transport.rdma_enabled` - _true_
* `protocol.v3_enabled` - _true_
* `protocol.v3_64bit_identifiers_enabled` - _false_
* `protocol.v4_id_domain` - defaultv4iddomain.com
* `protocol.v4_64bit_identifiers_enabled` - _true_
* `protocol.v4_enabled` - _false_
* `protocol.v41_enabled` - _false_
* `protocol.v40_features.acl_enabled` - _false_
* `protocol.v40_features.read_delegation_enabled` - _false_
* `protocol.v40_features.write_delegation_enabled` - _false_
* `protocol.v41_features.acl_enabled` - _false_
* `protocol.v41_features.read_delegation_enabled` - _false_
* `protocol.v41_features.write_delegation_enabled` - _false_
* `protocol.v41_features.pnfs_enabled` - _false_
* `vstorage_enabled` - _false_
* `rquota_enabled` - _false_
* `showmount_enabled` - _true_
* `auth_sys_extended_groups_enabled` - _false_
* `extended_groups_limit` - _32_
* `qtree.export_enabled` - _false_
* `qtree.validate_export` - _true_
* `access_cache_config.ttl_positive` - _60_
* `access_cache_config.ttl_negative` - _30_
* `access_cache_config.ttl_failure` - _1_
* `access_cache_config.harvest_timeout` - _3600_
* `access_cache_config.isDnsTTLEnabled` - _false_
* `file_session_io_grouping_count` - _5000_
* `file_session_io_grouping_duration` - _120_
* `security.nt_acl_display_permission` - _false_
* `exports.netgroup_trust_any_nsswitch_no_match` - _false_
* `exports.name_service_lookup_protocol` - _udp_
* `security.permitted_encryption_types` - [aes-256,aes-128,des3,des]
* `security.rpcsec_context_idle` - _0_
* `security.chown_mode` - _use_export_policy_
* `security.ntfs_unix_security` - _use_export_policy_
* `windows.v3_ms_dos_client_enabled` - _false_
* `windows.default_user` - ""
* `windows.map_unknown_uid_to_default_user` - _true_
* `credential_cache.positive_ttl` - _86400000_
* `credential_cache.negative_ttl` - _7200000_
* `credential_cache.transient_error_ttl` - _30000_
* `protocol.v40_features.acl_preserve` - _true_
* `protocol.v41_features.implementation_domain` - 'netapp.com'
* `protocol.v40_features.acl_max_aces` - _400_
* `protocol.v3_features.ejukebox_enabled` - _true_
* `protocol.v3_features.connection_drop` - _true_
* `protocol.v3_features.fsid_change` - _true_
* `protocol.v3_features.mount_daemon_port` - _635_
* `protocol.v3_features.network_lock_manager_port` - _4045_
* `protocol.v3_features.network_status_monitor_port` - _4046_
* `protocol.v3_features.rquota_daemon_port` - _4046_
* `protocol.v3_features.mount_root_only` - _true_
* `transport.tcp_max_transfer_size` - _65536_
* `root.ignore_nt_acl` - _false_
* `root.skip_write_permission_check` - _false_
* `protocol.v40_features.referrals_enabled` - _false_
* `protocol.v41_features.referrals_enabled` - _false_
* `protocol.v4_fsid_change` - _true_
* `protocol.v4_lease_seconds` - 30
* `protocol.v4_grace_seconds` - 45
* `protocol.v4_session_slots` - 180
* `protocol.v4_session_slot_reply_cache_size` - 640
### Related ONTAP commands
* `vserver nfs create`
* `export-policy access-cache config show`
### Learn more
* [`DOC /protocols/nfs/services`](#docs-NAS-protocols_nfs_services)
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
        records: Iterable["NfsService"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the NFS configuration of an SVM.
### Related ONTAP commands
* `vserver nfs delete`
### Learn more
* [`DOC /protocols/nfs/services`](#docs-NAS-protocols_nfs_services)
"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the NFS configuration of SVMs.
### Expensive properties
There is an added computational cost to retrieving values for these properties. They are not included by default in GET results and must be explicitly requested using the `fields` query parameter. See [`Requesting specific fields`](#Requesting_specific_fields) to learn more.
* `statistics.*`
* `metric.*`
### Advanced properties
* `security.rpcsec_context_idle`
* `security.ntfs_unix_security`
* `security.chown_mode`
* `security.nt_acl_display_permission`
* `protocol.v3_features.ejukebox_enabled`
* `protocol.v3_features.connection_drop`
* `protocol.v3_features.fsid_change`
* `protocol.v3_features.mount_daemon_port`
* `protocol.v3_features.network_lock_manager_port`
* `protocol.v3_features.network_status_monitor_port`
* `protocol.v3_features.rquota_daemon_port`
* `protocol.v3_features.hide_snapshot_enabled`
* `protocol.v41_features.implementation_domain`
* `protocol.v41_features.implementation_name`
* `protocol.v40_features.acl_max_aces`
* `windows.map_unknown_uid_to_default_user`
* `exports.netgroup_trust_any_nsswitch_no_match`
* `credential_cache.negative_ttl`
* `transport.tcp_max_transfer_size`
* `root.*`
* `protocol.v41_features.trunking_enabled`
* `protocol.v42_features.seclabel_enabled`
* `protocol.v42_features.sparsefile_ops_enabled`
* `protocol.v42_features.xattrs_enabled`
* `protocol.v40_features.referrals_enabled`
* `protocol.v41_features.referrals_enabled`
* `protocol.v4_lease_seconds`
* `protocol.v4_session_slots`
* `protocol.v4_session_slot_reply_cache_size`
* `protocol.v4_fsid_change`
### Diagnostic properties
* `credential_cache.transient_error_ttl`
* `access_cache_config.ttl_failure`
### Related ONTAP commands
* `vserver nfs show`
* `vserver nfs status`
### Learn more
* [`DOC /protocols/nfs/services`](#docs-NAS-protocols_nfs_services)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the NFS configuration of an SVM.
### Related ONTAP commands
* `vserver nfs show`
* `vserver nfs status`
### Learn more
* [`DOC /protocols/nfs/services`](#docs-NAS-protocols_nfs_services)
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
        r"""Creates an NFS configuration for an SVM.
### Required properties
* `svm.uuid` or `svm.name` - Existing SVM for which to create the NFS configuration.
### Default property values
If not specified in POST, the following default property values are assigned:
* `enabled` - _true_
* `state` - online
* `transport.udp_enabled` - _true_
* `transport.tcp_enabled` - _true_
* `transport.rdma_enabled` - _true_
* `protocol.v3_enabled` - _true_
* `protocol.v3_64bit_identifiers_enabled` - _false_
* `protocol.v4_id_domain` - defaultv4iddomain.com
* `protocol.v4_64bit_identifiers_enabled` - _true_
* `protocol.v4_enabled` - _false_
* `protocol.v41_enabled` - _false_
* `protocol.v40_features.acl_enabled` - _false_
* `protocol.v40_features.read_delegation_enabled` - _false_
* `protocol.v40_features.write_delegation_enabled` - _false_
* `protocol.v41_features.acl_enabled` - _false_
* `protocol.v41_features.read_delegation_enabled` - _false_
* `protocol.v41_features.write_delegation_enabled` - _false_
* `protocol.v41_features.pnfs_enabled` - _false_
* `vstorage_enabled` - _false_
* `rquota_enabled` - _false_
* `showmount_enabled` - _true_
* `auth_sys_extended_groups_enabled` - _false_
* `extended_groups_limit` - _32_
* `qtree.export_enabled` - _false_
* `qtree.validate_export` - _true_
* `access_cache_config.ttl_positive` - _60_
* `access_cache_config.ttl_negative` - _30_
* `access_cache_config.ttl_failure` - _1_
* `access_cache_config.harvest_timeout` - _3600_
* `access_cache_config.isDnsTTLEnabled` - _false_
* `file_session_io_grouping_count` - _5000_
* `file_session_io_grouping_duration` - _120_
* `security.nt_acl_display_permission` - _false_
* `exports.netgroup_trust_any_nsswitch_no_match` - _false_
* `exports.name_service_lookup_protocol` - _udp_
* `security.permitted_encryption_types` - [aes-256,aes-128,des3,des]
* `security.rpcsec_context_idle` - _0_
* `security.chown_mode` - _use_export_policy_
* `security.ntfs_unix_security` - _use_export_policy_
* `windows.v3_ms_dos_client_enabled` - _false_
* `windows.default_user` - ""
* `windows.map_unknown_uid_to_default_user` - _true_
* `credential_cache.positive_ttl` - _86400000_
* `credential_cache.negative_ttl` - _7200000_
* `credential_cache.transient_error_ttl` - _30000_
* `protocol.v40_features.acl_preserve` - _true_
* `protocol.v41_features.implementation_domain` - 'netapp.com'
* `protocol.v40_features.acl_max_aces` - _400_
* `protocol.v3_features.ejukebox_enabled` - _true_
* `protocol.v3_features.connection_drop` - _true_
* `protocol.v3_features.fsid_change` - _true_
* `protocol.v3_features.mount_daemon_port` - _635_
* `protocol.v3_features.network_lock_manager_port` - _4045_
* `protocol.v3_features.network_status_monitor_port` - _4046_
* `protocol.v3_features.rquota_daemon_port` - _4046_
* `protocol.v3_features.mount_root_only` - _true_
* `transport.tcp_max_transfer_size` - _65536_
* `root.ignore_nt_acl` - _false_
* `root.skip_write_permission_check` - _false_
* `protocol.v40_features.referrals_enabled` - _false_
* `protocol.v41_features.referrals_enabled` - _false_
* `protocol.v4_fsid_change` - _true_
* `protocol.v4_lease_seconds` - 30
* `protocol.v4_grace_seconds` - 45
* `protocol.v4_session_slots` - 180
* `protocol.v4_session_slot_reply_cache_size` - 640
### Related ONTAP commands
* `vserver nfs create`
* `export-policy access-cache config show`
### Learn more
* [`DOC /protocols/nfs/services`](#docs-NAS-protocols_nfs_services)
"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="nfs service create")
        async def nfs_service_create(
        ) -> ResourceTable:
            """Create an instance of a NfsService resource

            Args:
                links: 
                access_cache_config: 
                auth_sys_extended_groups_enabled: Specifies whether or not extended groups support over AUTH_SYS is enabled.
                credential_cache: 
                enabled: Specifies if the NFS service is administratively enabled. 
                exports: 
                extended_groups_limit: Specifies the maximum auxiliary groups supported over AUTH_SYS and RPCSEC_GSS.
                file_session_io_grouping_count: Number of I/O operations on a file to be grouped and considered as one session for event generation applications, such as FPolicy.
                file_session_io_grouping_duration: The duration for which I/O operations on a file will be grouped and considered as one session for event generation applications, such as FPolicy.
                metric: 
                protocol: 
                protocol_access_rules: 
                qtree: 
                root: 
                rquota_enabled: Specifies whether or not the remote quota feature is enabled.
                security: 
                showmount_enabled: Specifies whether or not the showmount feature is enabled.
                state: Specifies the state of the NFS service on the SVM. The following values are supported:           * online - NFS server is ready to accept client requests.           * offline - NFS server is not ready to accept client requests. 
                statistics: 
                svm: 
                transport: 
                vstorage_enabled: Specifies whether or not the VMware vstorage feature is enabled.
                windows: 
            """

            kwargs = {}
            if links is not None:
                kwargs["links"] = links
            if access_cache_config is not None:
                kwargs["access_cache_config"] = access_cache_config
            if auth_sys_extended_groups_enabled is not None:
                kwargs["auth_sys_extended_groups_enabled"] = auth_sys_extended_groups_enabled
            if credential_cache is not None:
                kwargs["credential_cache"] = credential_cache
            if enabled is not None:
                kwargs["enabled"] = enabled
            if exports is not None:
                kwargs["exports"] = exports
            if extended_groups_limit is not None:
                kwargs["extended_groups_limit"] = extended_groups_limit
            if file_session_io_grouping_count is not None:
                kwargs["file_session_io_grouping_count"] = file_session_io_grouping_count
            if file_session_io_grouping_duration is not None:
                kwargs["file_session_io_grouping_duration"] = file_session_io_grouping_duration
            if metric is not None:
                kwargs["metric"] = metric
            if protocol is not None:
                kwargs["protocol"] = protocol
            if protocol_access_rules is not None:
                kwargs["protocol_access_rules"] = protocol_access_rules
            if qtree is not None:
                kwargs["qtree"] = qtree
            if root is not None:
                kwargs["root"] = root
            if rquota_enabled is not None:
                kwargs["rquota_enabled"] = rquota_enabled
            if security is not None:
                kwargs["security"] = security
            if showmount_enabled is not None:
                kwargs["showmount_enabled"] = showmount_enabled
            if state is not None:
                kwargs["state"] = state
            if statistics is not None:
                kwargs["statistics"] = statistics
            if svm is not None:
                kwargs["svm"] = svm
            if transport is not None:
                kwargs["transport"] = transport
            if vstorage_enabled is not None:
                kwargs["vstorage_enabled"] = vstorage_enabled
            if windows is not None:
                kwargs["windows"] = windows

            resource = NfsService(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create NfsService: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates the NFS configuration of an SVM.
### Related ONTAP commands
* `vserver nfs modify`
* `vserver nfs on`
* `vserver nfs off`
* `vserver nfs start`
* `vserver nfs stop`
### Learn more
* [`DOC /protocols/nfs/services`](#docs-NAS-protocols_nfs_services)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="nfs service modify")
        async def nfs_service_modify(
        ) -> ResourceTable:
            """Modify an instance of a NfsService resource

            Args:
                auth_sys_extended_groups_enabled: Specifies whether or not extended groups support over AUTH_SYS is enabled.
                query_auth_sys_extended_groups_enabled: Specifies whether or not extended groups support over AUTH_SYS is enabled.
                enabled: Specifies if the NFS service is administratively enabled. 
                query_enabled: Specifies if the NFS service is administratively enabled. 
                extended_groups_limit: Specifies the maximum auxiliary groups supported over AUTH_SYS and RPCSEC_GSS.
                query_extended_groups_limit: Specifies the maximum auxiliary groups supported over AUTH_SYS and RPCSEC_GSS.
                file_session_io_grouping_count: Number of I/O operations on a file to be grouped and considered as one session for event generation applications, such as FPolicy.
                query_file_session_io_grouping_count: Number of I/O operations on a file to be grouped and considered as one session for event generation applications, such as FPolicy.
                file_session_io_grouping_duration: The duration for which I/O operations on a file will be grouped and considered as one session for event generation applications, such as FPolicy.
                query_file_session_io_grouping_duration: The duration for which I/O operations on a file will be grouped and considered as one session for event generation applications, such as FPolicy.
                rquota_enabled: Specifies whether or not the remote quota feature is enabled.
                query_rquota_enabled: Specifies whether or not the remote quota feature is enabled.
                showmount_enabled: Specifies whether or not the showmount feature is enabled.
                query_showmount_enabled: Specifies whether or not the showmount feature is enabled.
                state: Specifies the state of the NFS service on the SVM. The following values are supported:           * online - NFS server is ready to accept client requests.           * offline - NFS server is not ready to accept client requests. 
                query_state: Specifies the state of the NFS service on the SVM. The following values are supported:           * online - NFS server is ready to accept client requests.           * offline - NFS server is not ready to accept client requests. 
                vstorage_enabled: Specifies whether or not the VMware vstorage feature is enabled.
                query_vstorage_enabled: Specifies whether or not the VMware vstorage feature is enabled.
            """

            kwargs = {}
            changes = {}
            if query_auth_sys_extended_groups_enabled is not None:
                kwargs["auth_sys_extended_groups_enabled"] = query_auth_sys_extended_groups_enabled
            if query_enabled is not None:
                kwargs["enabled"] = query_enabled
            if query_extended_groups_limit is not None:
                kwargs["extended_groups_limit"] = query_extended_groups_limit
            if query_file_session_io_grouping_count is not None:
                kwargs["file_session_io_grouping_count"] = query_file_session_io_grouping_count
            if query_file_session_io_grouping_duration is not None:
                kwargs["file_session_io_grouping_duration"] = query_file_session_io_grouping_duration
            if query_rquota_enabled is not None:
                kwargs["rquota_enabled"] = query_rquota_enabled
            if query_showmount_enabled is not None:
                kwargs["showmount_enabled"] = query_showmount_enabled
            if query_state is not None:
                kwargs["state"] = query_state
            if query_vstorage_enabled is not None:
                kwargs["vstorage_enabled"] = query_vstorage_enabled

            if auth_sys_extended_groups_enabled is not None:
                changes["auth_sys_extended_groups_enabled"] = auth_sys_extended_groups_enabled
            if enabled is not None:
                changes["enabled"] = enabled
            if extended_groups_limit is not None:
                changes["extended_groups_limit"] = extended_groups_limit
            if file_session_io_grouping_count is not None:
                changes["file_session_io_grouping_count"] = file_session_io_grouping_count
            if file_session_io_grouping_duration is not None:
                changes["file_session_io_grouping_duration"] = file_session_io_grouping_duration
            if rquota_enabled is not None:
                changes["rquota_enabled"] = rquota_enabled
            if showmount_enabled is not None:
                changes["showmount_enabled"] = showmount_enabled
            if state is not None:
                changes["state"] = state
            if vstorage_enabled is not None:
                changes["vstorage_enabled"] = vstorage_enabled

            if hasattr(NfsService, "find"):
                resource = NfsService.find(
                    **kwargs
                )
            else:
                resource = NfsService()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify NfsService: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the NFS configuration of an SVM.
### Related ONTAP commands
* `vserver nfs delete`
### Learn more
* [`DOC /protocols/nfs/services`](#docs-NAS-protocols_nfs_services)
"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="nfs service delete")
        async def nfs_service_delete(
        ) -> None:
            """Delete an instance of a NfsService resource

            Args:
                auth_sys_extended_groups_enabled: Specifies whether or not extended groups support over AUTH_SYS is enabled.
                enabled: Specifies if the NFS service is administratively enabled. 
                extended_groups_limit: Specifies the maximum auxiliary groups supported over AUTH_SYS and RPCSEC_GSS.
                file_session_io_grouping_count: Number of I/O operations on a file to be grouped and considered as one session for event generation applications, such as FPolicy.
                file_session_io_grouping_duration: The duration for which I/O operations on a file will be grouped and considered as one session for event generation applications, such as FPolicy.
                rquota_enabled: Specifies whether or not the remote quota feature is enabled.
                showmount_enabled: Specifies whether or not the showmount feature is enabled.
                state: Specifies the state of the NFS service on the SVM. The following values are supported:           * online - NFS server is ready to accept client requests.           * offline - NFS server is not ready to accept client requests. 
                vstorage_enabled: Specifies whether or not the VMware vstorage feature is enabled.
            """

            kwargs = {}
            if auth_sys_extended_groups_enabled is not None:
                kwargs["auth_sys_extended_groups_enabled"] = auth_sys_extended_groups_enabled
            if enabled is not None:
                kwargs["enabled"] = enabled
            if extended_groups_limit is not None:
                kwargs["extended_groups_limit"] = extended_groups_limit
            if file_session_io_grouping_count is not None:
                kwargs["file_session_io_grouping_count"] = file_session_io_grouping_count
            if file_session_io_grouping_duration is not None:
                kwargs["file_session_io_grouping_duration"] = file_session_io_grouping_duration
            if rquota_enabled is not None:
                kwargs["rquota_enabled"] = rquota_enabled
            if showmount_enabled is not None:
                kwargs["showmount_enabled"] = showmount_enabled
            if state is not None:
                kwargs["state"] = state
            if vstorage_enabled is not None:
                kwargs["vstorage_enabled"] = vstorage_enabled

            if hasattr(NfsService, "find"):
                resource = NfsService.find(
                    **kwargs
                )
            else:
                resource = NfsService()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete NfsService: %s" % err)


