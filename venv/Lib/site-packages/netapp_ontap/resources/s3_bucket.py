r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
An S3 bucket is a container of objects. Each bucket defines an object namespace. S3 server requests specify objects using a bucket-name and object-name pair. An object consists of data, along with optional metadata and access controls, that is accessible using a name. An object resides within a bucket. There can be more than one bucket in an S3 server. Buckets that are created for the server are associated with an S3 user that is created on the S3 server.
## Examples
### Retrieving all fields for all S3 buckets of a cluster
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3Bucket

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(S3Bucket.get_collection(fields="**")))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    S3Bucket(
        {
            "svm": {"name": "vs1", "uuid": "12f3ba4c-7ae0-11e9-8c06-0050568ea123"},
            "name": "bucket-2",
            "volume": {
                "name": "fg_oss_1558514455",
                "uuid": "51276f5f-7c6d-11e9-97e8-0050568ea123",
            },
            "qos_policy": {
                "uuid": "39ac471f-ff35-11e9-b0f9-005056a7ab52",
                "name": "vs0_auto_gen_policy_39a9522f_ff35_11e9_b0f9_005056a7ab52",
            },
            "size": 209715200,
            "comment": "S3 bucket.",
            "encryption": {"enabled": False},
            "uuid": "527812ab-7c6d-11e9-97e8-0050568ea123",
            "logical_used_size": 157286400,
        }
    ),
    S3Bucket(
        {
            "svm": {"name": "vs1", "uuid": "12f3ba4c-7ae0-11e9-8c06-0050568ea123"},
            "name": "bucket-1",
            "volume": {
                "name": "fg_oss_1558690256",
                "uuid": "a36a1ea7-7e06-11e9-97e8-0050568ea123",
            },
            "qos_policy": {
                "uuid": "39ac471f-ff35-11e9-b0f9-005056a7ab52",
                "name": "vs0_auto_gen_policy_39a9522f_ff35_11e9_b0f9_005056a7ab52",
            },
            "size": 1677721600,
            "comment": "bucket2",
            "encryption": {"enabled": False},
            "uuid": "a8234aec-7e06-11e9-97e8-0050568ea123",
            "logical_used_size": 0,
        }
    ),
    S3Bucket(
        {
            "svm": {"name": "vs2", "uuid": "ee30eb2d-7ae1-11e9-8abe-0050568ea123"},
            "name": "bucket-3",
            "volume": {
                "name": "fg_oss_1558690257",
                "uuid": "a46a1ea7-7e06-11e9-97e8-0050568ea123",
            },
            "qos_policy": {
                "uuid": "39ac471f-ff35-11e9-b0f9-005056a7ab52",
                "name": "vs0_auto_gen_policy_39a9522f_ff35_11e9_b0f9_005056a7ab52",
            },
            "size": 1677721600,
            "policy": {
                "statements": [
                    {
                        "actions": ["*"],
                        "conditions": [
                            {"source_ips": ["1.1.1.1/10"], "operator": "ip_address"}
                        ],
                        "principals": ["Alice"],
                        "sid": "fullAccessForAliceToBucket",
                        "effect": "allow",
                        "resources": ["bucket-3", "bucket-3/*"],
                    }
                ]
            },
            "comment": "bucket3",
            "encryption": {"enabled": False},
            "uuid": "19283b75-7ae2-11e9-8abe-0050568ea123",
            "logical_used_size": 1075838976,
        }
    ),
]

```
</div>
</div>

### Retrieving all S3 buckets of a cluster ordered by size
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3Bucket

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(S3Bucket.get_collection(order_by="size")))

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
[
    S3Bucket(
        {
            "svm": {"name": "vs1", "uuid": "12f3ba4c-7ae0-11e9-8c06-0050568ea123"},
            "name": "bb1",
            "size": 83886080,
            "uuid": "754389d0-7e13-11e9-bfdc-0050568ea123",
        }
    ),
    S3Bucket(
        {
            "svm": {"name": "vs2", "uuid": "ee30eb2d-7ae1-11e9-8abe-0050568ea123"},
            "name": "bb2",
            "size": 838860800,
            "uuid": "19283b75-7ae2-11e9-8abe-0050568ea123",
        }
    ),
    S3Bucket(
        {
            "svm": {"name": "vs1", "uuid": "12f3ba4c-7ae0-11e9-8c06-0050568ea123"},
            "name": "bucket-1",
            "size": 1677721600,
            "uuid": "a8234aec-7e06-11e9-97e8-0050568ea123",
        }
    ),
]

```
</div>
</div>

### Retrieving all S3 buckets of a cluster with name  "bb2"
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3Bucket

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(S3Bucket.get_collection(name="bb2")))

```
<div class="try_it_out">
<input id="example2_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example2_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example2_result" class="try_it_out_content">
```
[
    S3Bucket(
        {
            "svm": {"name": "vs1", "uuid": "12f3ba4c-7ae0-11e9-8c06-0050568ea123"},
            "name": "bb2",
            "uuid": "087d940e-7e15-11e9-bfdc-0050568ea123",
        }
    ),
    S3Bucket(
        {
            "svm": {"name": "vs2", "uuid": "ee30eb2d-7ae1-11e9-8abe-0050568ea123"},
            "name": "bb2",
            "uuid": "19283b75-7ae2-11e9-8abe-0050568ea123",
        }
    ),
]

```
</div>
</div>

### Retrieving the specified bucket associated with an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3Bucket

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = S3Bucket(
        uuid="527812ab-7c6d-11e9-97e8-0050568ea123",
        **{"svm.uuid": "12f3ba4c-7ae0-11e9-8c06-0050568ea123"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example3_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example3_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example3_result" class="try_it_out_content">
```
S3Bucket(
    {
        "svm": {"name": "vs1", "uuid": "12f3ba4c-7ae0-11e9-8c06-0050568ea123"},
        "name": "bucket-2",
        "volume": {
            "name": "fg_oss_1558514455",
            "uuid": "51276f5f-7c6d-11e9-97e8-0050568ea123",
        },
        "qos_policy": {
            "uuid": "39ac471f-ff35-11e9-b0f9-005056a7ab52",
            "name": "vs0_auto_gen_policy_39a9522f_ff35_11e9_b0f9_005056a7ab52",
        },
        "size": 209715200,
        "comment": "S3 bucket.",
        "encryption": {"enabled": False},
        "uuid": "527812ab-7c6d-11e9-97e8-0050568ea123",
        "logical_used_size": 157286400,
    }
)

```
</div>
</div>

### Creating an S3 bucket for an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3Bucket

with HostConnection("<mgmt-ip>", username="admin", password="netapp1!", verify=False):
    resource = S3Bucket()
    resource.aggregates = [
        {"name": "aggr5", "uuid": "12f3ba4c-7ae0-11e9-8c06-0050568ea123"}
    ]
    resource.comment = "S3 bucket."
    resource.constituents_per_aggregate = 4
    resource.name = "bucket-3"
    resource.svm = {"name": "vs1"}
    resource.post(hydrate=True, return_timeout=0)
    print(resource)

```
<div class="try_it_out">
<input id="example4_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example4_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example4_result" class="try_it_out_content">
```
S3Bucket({"name": "bucket-3", "comment": "S3 bucket."})

```
</div>
</div>

### Creating an S3 bucket along with QoS policy for an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3Bucket

with HostConnection("<mgmt-ip>", username="admin", password="netapp1!", verify=False):
    resource = S3Bucket()
    resource.comment = "S3 bucket."
    resource.name = "bucket-3"
    resource.svm = {"name": "vs1"}
    resource.qos_policy = {
        "min_throughput_iops": 0,
        "min_throughput_mbps": 0,
        "max_throughput_iops": 1000000,
        "max_throughput_mbps": 900000,
        "uuid": "02d07a93-6177-11ea-b241-000c293feac8",
        "name": "vs0_auto_gen_policy_02cfa02a_6177_11ea_b241_000c293feac8",
    }
    resource.post(hydrate=True, return_timeout=0)
    print(resource)

```
<div class="try_it_out">
<input id="example5_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example5_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example5_result" class="try_it_out_content">
```
S3Bucket({"name": "bucket-3", "comment": "S3 bucket."})

```
</div>
</div>

### Creating an S3 bucket along with policies and conditions for an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3Bucket

with HostConnection("<mgmt-ip>", username="admin", password="netapp1!", verify=False):
    resource = S3Bucket()
    resource.aggregates = [
        {"name": "aggr5", "uuid": "12f3ba4c-7ae0-11e9-8c06-0050568ea123"}
    ]
    resource.comment = "S3 bucket."
    resource.constituents_per_aggregate = 4
    resource.name = "bucket-3"
    resource.policy = {
        "statements": [
            {
                "actions": ["GetObject"],
                "conditions": [
                    {
                        "operator": "ip_address",
                        "source_ips": ["1.1.1.1/23", "1.2.2.2/20"],
                    }
                ],
                "effect": "allow",
                "resources": ["bucket-3/policies/examples/*"],
                "sid": "AccessToGetObjectForAllUsersofSVM",
            },
            {
                "actions": ["*Object"],
                "effect": "deny",
                "principals": ["mike"],
                "resources": ["bucket-3/policy-docs/*", "bucket-3/confidential-*"],
                "sid": "DenyAccessToObjectForMike",
            },
            {
                "actions": ["GetObject"],
                "effect": "allow",
                "principals": ["*"],
                "resources": ["bucket-3/readme"],
                "sid": "AnonnymousAccessToGetObjectForUsers",
            },
        ]
    }
    resource.svm = {"uuid": "259b4e46-2d33-11ea-9145-005056bbbec1"}
    resource.post(hydrate=True, return_timeout=0)
    print(resource)

```
<div class="try_it_out">
<input id="example6_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example6_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example6_result" class="try_it_out_content">
```
S3Bucket({"name": "bucket-3", "comment": "S3 bucket."})

```
</div>
</div>

### Creating an S3 bucket along with a bucket lifecycle management rule for an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3Bucket

with HostConnection("<mgmt-ip>", username="admin", password="netapp1!", verify=False):
    resource = S3Bucket()
    resource.aggregates = [
        {"name": "aggr5", "uuid": "12f3ba4c-7ae0-11e9-8c06-0050568ea123"}
    ]
    resource.comment = "S3 bucket."
    resource.constituents_per_aggregate = 4
    resource.name = "bucket-4"
    resource.lifecycle_management = {
        "rules": [
            {
                "name": "rule1",
                "expiration": {"object_age_days": "1000"},
                "abort_incomplete_multipart_upload": {"after_initiation_days": 200},
                "object_filter": {"prefix": "obj1*/", "size_greater_than": "1000"},
            },
            {
                "name": "rule2",
                "object_filter": {"size_greater_than": "50"},
                "expiration": {"object_age_days": "5000"},
            },
        ]
    }
    resource.post(hydrate=True, return_timeout=0)
    print(resource)

```
<div class="try_it_out">
<input id="example7_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example7_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example7_result" class="try_it_out_content">
```
S3Bucket({"name": "bucket-4", "comment": "S3 bucket."})

```
</div>
</div>

# The API:
/api/protocols/s3/buckets/{svm.uuid}/{uuid}
# The call:
curl -X PATCH "https://<mgmt-ip>/api/protocols/s3/buckets/259b4e46-2d33-11ea-9145-005056bbbec1/376a2efd-2d4d-11ea-9c30-005056bb883a?return_records=true" -H "accept: application/json?return_records=true" -H "Content-Type: application/json" -d "{ \"comment\": \"Bucket modified.\", \"size\": 111111111111,  \"qos_policy\": { \"min_throughput_iops\": 0, \"min_throughput_mbps\": 0, \"max_throughput_iops\": 1000000, \"max_throughput_mbps\": 900000, \"uuid\": \"02d07a93-6177-11ea-b241-000c293feac8\", \"name\": \"vs0_auto_gen_policy_02cfa02a_6177_11ea_b241_000c293feac8\" }}"
# The response:
HTTP/1.1 202 Accepted
Date: Fri, 24 May 2019 11:32:27 GMT
Server: libzapid-httpd
X-Content-Type-Options: nosniff
Cache-Control: no-cache,no-store,must-revalidate
Content-Length: 189
Content-Type: application/json
{
  "job": {
    "uuid": "9beafabb-7e17-11e9-bfdc-0050568ea123",
    "_links": {
      "self": {
        "href": "/api/cluster/jobs/9beafabb-7e17-11e9-bfdc-0050568ea123"
      }
    }
  }
}
```
### Updating an S3 bucket policy for an SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3Bucket

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = S3Bucket(
        uuid="376a2efd-2d4d-11ea-9c30-005056bb883a",
        **{"svm.uuid": "259b4e46-2d33-11ea-9145-005056bbbec1"}
    )
    resource.policy = {
        "statements": [
            {
                "actions": ["*"],
                "conditions": [
                    {"operator": "ip_address", "source_ips": ["1.1.1.5/23"]}
                ],
                "effect": "allow",
                "resources": ["*"],
                "sid": "fullAccessForAllPrincipalsToBucket",
            }
        ]
    }
    resource.patch()

```

### Deleting an S3 bucket for a specified SVM
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import S3Bucket

with HostConnection("<mgmt-ip>", username="admin", password="netapp1!", verify=False):
    resource = S3Bucket(
        uuid="98528221-2d52-11ea-892e-005056bbbec1",
        **{"svm.uuid": "259b4e46-2d33-11ea-9145-005056bbbec1"}
    )
    resource.delete()

```
"""

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


__all__ = ["S3Bucket", "S3BucketSchema"]
__pdoc__ = {
    "S3BucketSchema.resource": False,
    "S3BucketSchema.opts": False,
    "S3Bucket.s3_bucket_show": False,
    "S3Bucket.s3_bucket_create": False,
    "S3Bucket.s3_bucket_modify": False,
    "S3Bucket.s3_bucket_delete": False,
}


class S3BucketSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3Bucket object"""

    aggregates = fields.List(fields.Nested("netapp_ontap.models.flexcache_aggregates.FlexcacheAggregatesSchema", unknown=EXCLUDE), data_key="aggregates")
    r""" A list of aggregates for FlexGroup volume constituents where the bucket is hosted. If this option is not specified, the bucket is auto-provisioned as a FlexGroup volume."""

    allowed = fields.Boolean(
        data_key="allowed",
    )
    r""" If this is set to true, an SVM administrator can manage the S3 service. If it is false, only the cluster administrator can manage the service."""

    audit_event_selector = fields.Nested("netapp_ontap.models.s3_audit_event_selector.S3AuditEventSelectorSchema", data_key="audit_event_selector", unknown=EXCLUDE)
    r""" The audit_event_selector field of the s3_bucket."""

    comment = fields.Str(
        data_key="comment",
        validate=len_validation(minimum=0, maximum=256),
    )
    r""" Can contain any additional information about the bucket being created or modified.

Example: S3 bucket."""

    constituents_per_aggregate = Size(
        data_key="constituents_per_aggregate",
        validate=integer_validation(minimum=1, maximum=1000),
    )
    r""" Specifies the number of constituents or FlexVol volumes per aggregate. A FlexGroup volume consisting of all such constituents across all specified aggregates is created. This option is used along with the aggregates option and cannot be used independently.

Example: 4"""

    encryption = fields.Nested("netapp_ontap.models.s3_bucket_encryption.S3BucketEncryptionSchema", data_key="encryption", unknown=EXCLUDE)
    r""" The encryption field of the s3_bucket."""

    lifecycle_management = fields.Nested("netapp_ontap.models.s3_bucket_lifecycle_management.S3BucketLifecycleManagementSchema", data_key="lifecycle_management", unknown=EXCLUDE)
    r""" The lifecycle_management field of the s3_bucket."""

    logical_used_size = Size(
        data_key="logical_used_size",
    )
    r""" Specifies the bucket logical used size up to this point."""

    name = fields.Str(
        data_key="name",
        validate=len_validation(minimum=3, maximum=63),
    )
    r""" Specifies the name of the bucket. Bucket name is a string that can only contain the following combination of ASCII-range alphanumeric characters 0-9, a-z, ".", and "-".

Example: bucket1"""

    nas_path = fields.Str(
        data_key="nas_path",
    )
    r""" Specifies the NAS path to which the nas bucket corresponds to.

Example: /"""

    policy = fields.Nested("netapp_ontap.models.s3_bucket_policy.S3BucketPolicySchema", data_key="policy", unknown=EXCLUDE)
    r""" The policy field of the s3_bucket."""

    protection_status = fields.Nested("netapp_ontap.models.s3_bucket_protection_status.S3BucketProtectionStatusSchema", data_key="protection_status", unknown=EXCLUDE)
    r""" The protection_status field of the s3_bucket."""

    qos_policy = fields.Nested("netapp_ontap.resources.qos_policy.QosPolicySchema", data_key="qos_policy", unknown=EXCLUDE)
    r""" The qos_policy field of the s3_bucket."""

    role = fields.Str(
        data_key="role",
        validate=enum_validation(['standalone', 'active', 'passive']),
    )
    r""" Specifies the role of the bucket.

Valid choices:

* standalone
* active
* passive"""

    size = Size(
        data_key="size",
        validate=integer_validation(minimum=83886080, maximum=70368744177664),
    )
    r""" Specifies the bucket size in bytes; ranges from 80MB to 64TB.

Example: 1677721600"""

    storage_service_level = fields.Str(
        data_key="storage_service_level",
        validate=enum_validation(['value', 'performance', 'extreme']),
    )
    r""" Specifies the storage service level of the FlexGroup volume on which the bucket should be created. Valid values are "value", "performance" or "extreme".

Valid choices:

* value
* performance
* extreme"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the s3_bucket."""

    type = fields.Str(
        data_key="type",
        validate=enum_validation(['s3', 'nas']),
    )
    r""" Specifies the bucket type. Valid values are "s3"and "nas".

Valid choices:

* s3
* nas"""

    uuid = fields.Str(
        data_key="uuid",
    )
    r""" Specifies the unique identifier of the bucket.

Example: 414b29a1-3b26-11e9-bd58-0050568ea055"""

    versioning_state = fields.Str(
        data_key="versioning_state",
        validate=enum_validation(['disabled', 'enabled', 'suspended']),
    )
    r""" Specifies the versioning state of the bucket. Valid values are "disabled", "enabled" or "suspended". Note that the versioning state cannot be modified to 'disabled' from any other state.

Valid choices:

* disabled
* enabled
* suspended"""

    volume = fields.Nested("netapp_ontap.resources.volume.VolumeSchema", data_key="volume", unknown=EXCLUDE)
    r""" The volume field of the s3_bucket."""

    @property
    def resource(self):
        return S3Bucket

    gettable_fields = [
        "allowed",
        "audit_event_selector",
        "comment",
        "encryption",
        "lifecycle_management",
        "logical_used_size",
        "name",
        "nas_path",
        "policy",
        "protection_status",
        "qos_policy.links",
        "qos_policy.max_throughput_iops",
        "qos_policy.max_throughput_mbps",
        "qos_policy.min_throughput_iops",
        "qos_policy.min_throughput_mbps",
        "qos_policy.name",
        "qos_policy.uuid",
        "role",
        "size",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "type",
        "uuid",
        "versioning_state",
        "volume.links",
        "volume.name",
        "volume.uuid",
    ]
    """allowed,audit_event_selector,comment,encryption,lifecycle_management,logical_used_size,name,nas_path,policy,protection_status,qos_policy.links,qos_policy.max_throughput_iops,qos_policy.max_throughput_mbps,qos_policy.min_throughput_iops,qos_policy.min_throughput_mbps,qos_policy.name,qos_policy.uuid,role,size,svm.links,svm.name,svm.uuid,type,uuid,versioning_state,volume.links,volume.name,volume.uuid,"""

    patchable_fields = [
        "allowed",
        "audit_event_selector",
        "comment",
        "encryption",
        "nas_path",
        "policy",
        "protection_status",
        "qos_policy.max_throughput_iops",
        "qos_policy.max_throughput_mbps",
        "qos_policy.min_throughput_iops",
        "qos_policy.min_throughput_mbps",
        "qos_policy.name",
        "qos_policy.uuid",
        "size",
        "type",
        "versioning_state",
        "volume.name",
        "volume.uuid",
    ]
    """allowed,audit_event_selector,comment,encryption,nas_path,policy,protection_status,qos_policy.max_throughput_iops,qos_policy.max_throughput_mbps,qos_policy.min_throughput_iops,qos_policy.min_throughput_mbps,qos_policy.name,qos_policy.uuid,size,type,versioning_state,volume.name,volume.uuid,"""

    postable_fields = [
        "aggregates.links",
        "aggregates.name",
        "aggregates.uuid",
        "allowed",
        "audit_event_selector",
        "comment",
        "constituents_per_aggregate",
        "encryption",
        "lifecycle_management",
        "name",
        "nas_path",
        "policy",
        "protection_status",
        "qos_policy.max_throughput_iops",
        "qos_policy.max_throughput_mbps",
        "qos_policy.min_throughput_iops",
        "qos_policy.min_throughput_mbps",
        "qos_policy.name",
        "qos_policy.uuid",
        "size",
        "storage_service_level",
        "svm.name",
        "svm.uuid",
        "type",
        "versioning_state",
        "volume.name",
        "volume.uuid",
    ]
    """aggregates.links,aggregates.name,aggregates.uuid,allowed,audit_event_selector,comment,constituents_per_aggregate,encryption,lifecycle_management,name,nas_path,policy,protection_status,qos_policy.max_throughput_iops,qos_policy.max_throughput_mbps,qos_policy.min_throughput_iops,qos_policy.min_throughput_mbps,qos_policy.name,qos_policy.uuid,size,storage_service_level,svm.name,svm.uuid,type,versioning_state,volume.name,volume.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in S3Bucket.get_collection(fields=field)]
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
            raise NetAppRestError("S3Bucket modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class S3Bucket(Resource):
    r""" A bucket is a container of objects. Each bucket defines an object namespace. S3 requests specify objects using a bucket-name and object-name pair. An object resides within a bucket. """

    _schema = S3BucketSchema
    _path = "/api/protocols/s3/buckets"
    _keys = ["svm.uuid", "uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves all S3 buckets for all SVMs. Note that in order to retrieve S3 bucket policy conditions, the 'fields' option should be set to '**'.
### Related ONTAP commands
* `vserver object-store-server bucket show`
* `vserver object-store-server bucket policy statement show`
* `vserver object-store-server bucket policy-statement-condition show`
* `vserver object-store-server bucket lifecycle-management-rule show`
### Learn more
* [`DOC /protocols/s3/buckets`](#docs-object-store-protocols_s3_buckets)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="s3 bucket show")
        def s3_bucket_show(
            fields: List[Choices.define(["allowed", "comment", "constituents_per_aggregate", "logical_used_size", "name", "nas_path", "role", "size", "storage_service_level", "type", "uuid", "versioning_state", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of S3Bucket resources

            Args:
                allowed: If this is set to true, an SVM administrator can manage the S3 service. If it is false, only the cluster administrator can manage the service.
                comment: Can contain any additional information about the bucket being created or modified.
                constituents_per_aggregate: Specifies the number of constituents or FlexVol volumes per aggregate. A FlexGroup volume consisting of all such constituents across all specified aggregates is created. This option is used along with the aggregates option and cannot be used independently.
                logical_used_size: Specifies the bucket logical used size up to this point.
                name: Specifies the name of the bucket. Bucket name is a string that can only contain the following combination of ASCII-range alphanumeric characters 0-9, a-z, \".\", and \"-\".
                nas_path: Specifies the NAS path to which the nas bucket corresponds to.
                role: Specifies the role of the bucket.
                size: Specifies the bucket size in bytes; ranges from 80MB to 64TB.
                storage_service_level: Specifies the storage service level of the FlexGroup volume on which the bucket should be created. Valid values are \"value\", \"performance\" or \"extreme\".
                type: Specifies the bucket type. Valid values are \"s3\"and \"nas\".
                uuid: Specifies the unique identifier of the bucket.
                versioning_state: Specifies the versioning state of the bucket. Valid values are \"disabled\", \"enabled\" or \"suspended\". Note that the versioning state cannot be modified to 'disabled' from any other state.
            """

            kwargs = {}
            if allowed is not None:
                kwargs["allowed"] = allowed
            if comment is not None:
                kwargs["comment"] = comment
            if constituents_per_aggregate is not None:
                kwargs["constituents_per_aggregate"] = constituents_per_aggregate
            if logical_used_size is not None:
                kwargs["logical_used_size"] = logical_used_size
            if name is not None:
                kwargs["name"] = name
            if nas_path is not None:
                kwargs["nas_path"] = nas_path
            if role is not None:
                kwargs["role"] = role
            if size is not None:
                kwargs["size"] = size
            if storage_service_level is not None:
                kwargs["storage_service_level"] = storage_service_level
            if type is not None:
                kwargs["type"] = type
            if uuid is not None:
                kwargs["uuid"] = uuid
            if versioning_state is not None:
                kwargs["versioning_state"] = versioning_state
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return S3Bucket.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all S3Bucket resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["S3Bucket"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates the S3 bucket configuration of an SVM.
### Important notes
- The following fields can be modified for a bucket:
  * `comment` - Any information related to the bucket.
  * `size` - Bucket size.
  * `policy` - An access policy for resources (buckets and objects) that defines their permissions. New policies are created after existing policies are deleted. To retain any of the existing policy statements, you need to specify those statements again. Also, policy conditions can be specified as part of a bucket policy.
  * `qos_policy` - A QoS policy for buckets.
  * `audit_event_selector` - Audit policy for buckets. None can be specified for both access and permission to remove an audit event selector.
  * `versioning-state` - Versioning state of the buckets.
  * `nas_path` - NAS path to which the bucket corresponds to.
### Related ONTAP commands
* `vserver object-store-server bucket modify`
* `vserver object-store-server bucket policy statement modify`
* `vserver object-store-server bucket policy-statement-condition modify`
### Learn more
* [`DOC /protocols/s3/buckets`](#docs-object-store-protocols_s3_buckets)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["S3Bucket"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["S3Bucket"], NetAppResponse]:
        r"""Creates the S3 bucket configuration of an SVM.
### Important notes
- Each SVM can have one or more bucket configurations.
- Aggregate lists should be specified explicitly. If not specified, then the bucket is auto-provisioned as a FlexGroup volume.
- Constituents per aggregate specifies the number of components (or FlexVol volumes) per aggregate. Is specified only when an aggregate list is explicitly defined.
- An access policy can be created along with a bucket create. If creating an access policy fails, bucket configurations are saved and the access policy can be created using the PATCH endpoint.
- "qos_policy" can be specified if a bucket needs to be attached to a QoS group policy during creation time.
- "audit_event_selector" can be specified if a bucket needs to be specify access and permission type for auditing.
### Required properties
* `svm.uuid or svm.name` - Existing SVM in which to create the bucket configuration.
* `name` - Bucket name that is to be created.
### Recommended optional properties
* `aggregates` - List of aggregates for the FlexGroup volume on which the bucket is hosted on.
* `constituents_per_aggregate` - Number of constituents per aggregate.
* `size` - Specifying the bucket size is recommended.
* `policy` - Specifying a policy enables users to perform operations on buckets; specifying the resource permissions is recommended.
* `qos_policy` - A QoS policy for buckets.
* `audit_event_selector` - Audit policy for buckets.
* `versioning_state` - Versioning state for buckets.
* `type` - Type of bucket.
* `nas_path` - NAS path to which the bucket corresponds to.

* `lifecycle_management` - Object store server lifecycle management policy.
### Default property values
* `size` - 800MB
* `comment` - ""
* `aggregates` - No default value.
* `constituents_per_aggregate` - _4_ , if an aggregates list is specified. Otherwise, no default value.
* `policy.statements.actions` - GetObject, PutObject, DeleteObject, ListBucket, ListBucketMultipartUploads, ListMultipartUploadParts, GetObjectTagging, PutObjectTagging, DeleteObjectTagging, GetBucketVersioning, PutBucketVersioning.
* `policy.statements.principals` - all S3 users and groups in the SVM.
* `policy.statements.resources` - all objects in the bucket.
* `policy.statements.conditions` - list of bucket policy conditions.
* `versioning_state` - disabled.
* `type` - S3
### Related ONTAP commands
* `vserver object-store-server bucket create`
* `vserver object-store-server bucket policy statement create`
* `vserver object-store-server bucket lifecycle-management-rule create`
### Learn more
* [`DOC /protocols/s3/buckets`](#docs-object-store-protocols_s3_buckets)
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
        records: Iterable["S3Bucket"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the S3 bucket configuration of an SVM. An access policy is also deleted on an S3 bucket "delete" command.
### Related ONTAP commands
* `vserver object-store-server bucket delete`
* `vserver object-store-server bucket policy statement delete`
* `vserver object-store-server bucket policy-statement-condition delete`
* `vserver object-store-server bucket lifecycle-management-rule delete`
### Learn more
* [`DOC /protocols/s3/buckets`](#docs-object-store-protocols_s3_buckets)
"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves all S3 buckets for all SVMs. Note that in order to retrieve S3 bucket policy conditions, the 'fields' option should be set to '**'.
### Related ONTAP commands
* `vserver object-store-server bucket show`
* `vserver object-store-server bucket policy statement show`
* `vserver object-store-server bucket policy-statement-condition show`
* `vserver object-store-server bucket lifecycle-management-rule show`
### Learn more
* [`DOC /protocols/s3/buckets`](#docs-object-store-protocols_s3_buckets)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the S3 bucket configuration of an SVM. Note that in order to retrieve S3 bucket policy conditions, the 'fields' option should be set to '**'.
### Related ONTAP commands
* `vserver object-store-server bucket show`
* `vserver object-store-server bucket policy statement show`
* `vserver object-store-server bucket policy-statement-condition show`
* `vserver object-store-server bucket lifecycle-management-rule show`
### Learn more
* [`DOC /protocols/s3/buckets`](#docs-object-store-protocols_s3_buckets)
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
        r"""Creates the S3 bucket configuration of an SVM.
### Important notes
- Each SVM can have one or more bucket configurations.
- Aggregate lists should be specified explicitly. If not specified, then the bucket is auto-provisioned as a FlexGroup volume.
- Constituents per aggregate specifies the number of components (or FlexVol volumes) per aggregate. Is specified only when an aggregate list is explicitly defined.
- An access policy can be created along with a bucket create. If creating an access policy fails, bucket configurations are saved and the access policy can be created using the PATCH endpoint.
- "qos_policy" can be specified if a bucket needs to be attached to a QoS group policy during creation time.
- "audit_event_selector" can be specified if a bucket needs to be specify access and permission type for auditing.
### Required properties
* `svm.uuid or svm.name` - Existing SVM in which to create the bucket configuration.
* `name` - Bucket name that is to be created.
### Recommended optional properties
* `aggregates` - List of aggregates for the FlexGroup volume on which the bucket is hosted on.
* `constituents_per_aggregate` - Number of constituents per aggregate.
* `size` - Specifying the bucket size is recommended.
* `policy` - Specifying a policy enables users to perform operations on buckets; specifying the resource permissions is recommended.
* `qos_policy` - A QoS policy for buckets.
* `audit_event_selector` - Audit policy for buckets.
* `versioning_state` - Versioning state for buckets.
* `type` - Type of bucket.
* `nas_path` - NAS path to which the bucket corresponds to.

* `lifecycle_management` - Object store server lifecycle management policy.
### Default property values
* `size` - 800MB
* `comment` - ""
* `aggregates` - No default value.
* `constituents_per_aggregate` - _4_ , if an aggregates list is specified. Otherwise, no default value.
* `policy.statements.actions` - GetObject, PutObject, DeleteObject, ListBucket, ListBucketMultipartUploads, ListMultipartUploadParts, GetObjectTagging, PutObjectTagging, DeleteObjectTagging, GetBucketVersioning, PutBucketVersioning.
* `policy.statements.principals` - all S3 users and groups in the SVM.
* `policy.statements.resources` - all objects in the bucket.
* `policy.statements.conditions` - list of bucket policy conditions.
* `versioning_state` - disabled.
* `type` - S3
### Related ONTAP commands
* `vserver object-store-server bucket create`
* `vserver object-store-server bucket policy statement create`
* `vserver object-store-server bucket lifecycle-management-rule create`
### Learn more
* [`DOC /protocols/s3/buckets`](#docs-object-store-protocols_s3_buckets)
"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="s3 bucket create")
        async def s3_bucket_create(
        ) -> ResourceTable:
            """Create an instance of a S3Bucket resource

            Args:
                aggregates: A list of aggregates for FlexGroup volume constituents where the bucket is hosted. If this option is not specified, the bucket is auto-provisioned as a FlexGroup volume.
                allowed: If this is set to true, an SVM administrator can manage the S3 service. If it is false, only the cluster administrator can manage the service.
                audit_event_selector: 
                comment: Can contain any additional information about the bucket being created or modified.
                constituents_per_aggregate: Specifies the number of constituents or FlexVol volumes per aggregate. A FlexGroup volume consisting of all such constituents across all specified aggregates is created. This option is used along with the aggregates option and cannot be used independently.
                encryption: 
                lifecycle_management: 
                logical_used_size: Specifies the bucket logical used size up to this point.
                name: Specifies the name of the bucket. Bucket name is a string that can only contain the following combination of ASCII-range alphanumeric characters 0-9, a-z, \".\", and \"-\".
                nas_path: Specifies the NAS path to which the nas bucket corresponds to.
                policy: 
                protection_status: 
                qos_policy: 
                role: Specifies the role of the bucket.
                size: Specifies the bucket size in bytes; ranges from 80MB to 64TB.
                storage_service_level: Specifies the storage service level of the FlexGroup volume on which the bucket should be created. Valid values are \"value\", \"performance\" or \"extreme\".
                svm: 
                type: Specifies the bucket type. Valid values are \"s3\"and \"nas\".
                uuid: Specifies the unique identifier of the bucket.
                versioning_state: Specifies the versioning state of the bucket. Valid values are \"disabled\", \"enabled\" or \"suspended\". Note that the versioning state cannot be modified to 'disabled' from any other state.
                volume: 
            """

            kwargs = {}
            if aggregates is not None:
                kwargs["aggregates"] = aggregates
            if allowed is not None:
                kwargs["allowed"] = allowed
            if audit_event_selector is not None:
                kwargs["audit_event_selector"] = audit_event_selector
            if comment is not None:
                kwargs["comment"] = comment
            if constituents_per_aggregate is not None:
                kwargs["constituents_per_aggregate"] = constituents_per_aggregate
            if encryption is not None:
                kwargs["encryption"] = encryption
            if lifecycle_management is not None:
                kwargs["lifecycle_management"] = lifecycle_management
            if logical_used_size is not None:
                kwargs["logical_used_size"] = logical_used_size
            if name is not None:
                kwargs["name"] = name
            if nas_path is not None:
                kwargs["nas_path"] = nas_path
            if policy is not None:
                kwargs["policy"] = policy
            if protection_status is not None:
                kwargs["protection_status"] = protection_status
            if qos_policy is not None:
                kwargs["qos_policy"] = qos_policy
            if role is not None:
                kwargs["role"] = role
            if size is not None:
                kwargs["size"] = size
            if storage_service_level is not None:
                kwargs["storage_service_level"] = storage_service_level
            if svm is not None:
                kwargs["svm"] = svm
            if type is not None:
                kwargs["type"] = type
            if uuid is not None:
                kwargs["uuid"] = uuid
            if versioning_state is not None:
                kwargs["versioning_state"] = versioning_state
            if volume is not None:
                kwargs["volume"] = volume

            resource = S3Bucket(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create S3Bucket: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates the S3 bucket configuration of an SVM.
### Important notes
- The following fields can be modified for a bucket:
  * `comment` - Any information related to the bucket.
  * `size` - Bucket size.
  * `policy` - An access policy for resources (buckets and objects) that defines their permissions. New policies are created after existing policies are deleted. To retain any of the existing policy statements, you need to specify those statements again. Also, policy conditions can be specified as part of a bucket policy.
  * `qos_policy` - A QoS policy for buckets.
  * `audit_event_selector` - Audit policy for buckets. None can be specified for both access and permission to remove an audit event selector.
  * `versioning-state` - Versioning state of the buckets.
  * `nas_path` - NAS path to which the bucket corresponds to.
### Related ONTAP commands
* `vserver object-store-server bucket modify`
* `vserver object-store-server bucket policy statement modify`
* `vserver object-store-server bucket policy-statement-condition modify`
### Learn more
* [`DOC /protocols/s3/buckets`](#docs-object-store-protocols_s3_buckets)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="s3 bucket modify")
        async def s3_bucket_modify(
        ) -> ResourceTable:
            """Modify an instance of a S3Bucket resource

            Args:
                allowed: If this is set to true, an SVM administrator can manage the S3 service. If it is false, only the cluster administrator can manage the service.
                query_allowed: If this is set to true, an SVM administrator can manage the S3 service. If it is false, only the cluster administrator can manage the service.
                comment: Can contain any additional information about the bucket being created or modified.
                query_comment: Can contain any additional information about the bucket being created or modified.
                constituents_per_aggregate: Specifies the number of constituents or FlexVol volumes per aggregate. A FlexGroup volume consisting of all such constituents across all specified aggregates is created. This option is used along with the aggregates option and cannot be used independently.
                query_constituents_per_aggregate: Specifies the number of constituents or FlexVol volumes per aggregate. A FlexGroup volume consisting of all such constituents across all specified aggregates is created. This option is used along with the aggregates option and cannot be used independently.
                logical_used_size: Specifies the bucket logical used size up to this point.
                query_logical_used_size: Specifies the bucket logical used size up to this point.
                name: Specifies the name of the bucket. Bucket name is a string that can only contain the following combination of ASCII-range alphanumeric characters 0-9, a-z, \".\", and \"-\".
                query_name: Specifies the name of the bucket. Bucket name is a string that can only contain the following combination of ASCII-range alphanumeric characters 0-9, a-z, \".\", and \"-\".
                nas_path: Specifies the NAS path to which the nas bucket corresponds to.
                query_nas_path: Specifies the NAS path to which the nas bucket corresponds to.
                role: Specifies the role of the bucket.
                query_role: Specifies the role of the bucket.
                size: Specifies the bucket size in bytes; ranges from 80MB to 64TB.
                query_size: Specifies the bucket size in bytes; ranges from 80MB to 64TB.
                storage_service_level: Specifies the storage service level of the FlexGroup volume on which the bucket should be created. Valid values are \"value\", \"performance\" or \"extreme\".
                query_storage_service_level: Specifies the storage service level of the FlexGroup volume on which the bucket should be created. Valid values are \"value\", \"performance\" or \"extreme\".
                type: Specifies the bucket type. Valid values are \"s3\"and \"nas\".
                query_type: Specifies the bucket type. Valid values are \"s3\"and \"nas\".
                uuid: Specifies the unique identifier of the bucket.
                query_uuid: Specifies the unique identifier of the bucket.
                versioning_state: Specifies the versioning state of the bucket. Valid values are \"disabled\", \"enabled\" or \"suspended\". Note that the versioning state cannot be modified to 'disabled' from any other state.
                query_versioning_state: Specifies the versioning state of the bucket. Valid values are \"disabled\", \"enabled\" or \"suspended\". Note that the versioning state cannot be modified to 'disabled' from any other state.
            """

            kwargs = {}
            changes = {}
            if query_allowed is not None:
                kwargs["allowed"] = query_allowed
            if query_comment is not None:
                kwargs["comment"] = query_comment
            if query_constituents_per_aggregate is not None:
                kwargs["constituents_per_aggregate"] = query_constituents_per_aggregate
            if query_logical_used_size is not None:
                kwargs["logical_used_size"] = query_logical_used_size
            if query_name is not None:
                kwargs["name"] = query_name
            if query_nas_path is not None:
                kwargs["nas_path"] = query_nas_path
            if query_role is not None:
                kwargs["role"] = query_role
            if query_size is not None:
                kwargs["size"] = query_size
            if query_storage_service_level is not None:
                kwargs["storage_service_level"] = query_storage_service_level
            if query_type is not None:
                kwargs["type"] = query_type
            if query_uuid is not None:
                kwargs["uuid"] = query_uuid
            if query_versioning_state is not None:
                kwargs["versioning_state"] = query_versioning_state

            if allowed is not None:
                changes["allowed"] = allowed
            if comment is not None:
                changes["comment"] = comment
            if constituents_per_aggregate is not None:
                changes["constituents_per_aggregate"] = constituents_per_aggregate
            if logical_used_size is not None:
                changes["logical_used_size"] = logical_used_size
            if name is not None:
                changes["name"] = name
            if nas_path is not None:
                changes["nas_path"] = nas_path
            if role is not None:
                changes["role"] = role
            if size is not None:
                changes["size"] = size
            if storage_service_level is not None:
                changes["storage_service_level"] = storage_service_level
            if type is not None:
                changes["type"] = type
            if uuid is not None:
                changes["uuid"] = uuid
            if versioning_state is not None:
                changes["versioning_state"] = versioning_state

            if hasattr(S3Bucket, "find"):
                resource = S3Bucket.find(
                    **kwargs
                )
            else:
                resource = S3Bucket()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify S3Bucket: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the S3 bucket configuration of an SVM. An access policy is also deleted on an S3 bucket "delete" command.
### Related ONTAP commands
* `vserver object-store-server bucket delete`
* `vserver object-store-server bucket policy statement delete`
* `vserver object-store-server bucket policy-statement-condition delete`
* `vserver object-store-server bucket lifecycle-management-rule delete`
### Learn more
* [`DOC /protocols/s3/buckets`](#docs-object-store-protocols_s3_buckets)
"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="s3 bucket delete")
        async def s3_bucket_delete(
        ) -> None:
            """Delete an instance of a S3Bucket resource

            Args:
                allowed: If this is set to true, an SVM administrator can manage the S3 service. If it is false, only the cluster administrator can manage the service.
                comment: Can contain any additional information about the bucket being created or modified.
                constituents_per_aggregate: Specifies the number of constituents or FlexVol volumes per aggregate. A FlexGroup volume consisting of all such constituents across all specified aggregates is created. This option is used along with the aggregates option and cannot be used independently.
                logical_used_size: Specifies the bucket logical used size up to this point.
                name: Specifies the name of the bucket. Bucket name is a string that can only contain the following combination of ASCII-range alphanumeric characters 0-9, a-z, \".\", and \"-\".
                nas_path: Specifies the NAS path to which the nas bucket corresponds to.
                role: Specifies the role of the bucket.
                size: Specifies the bucket size in bytes; ranges from 80MB to 64TB.
                storage_service_level: Specifies the storage service level of the FlexGroup volume on which the bucket should be created. Valid values are \"value\", \"performance\" or \"extreme\".
                type: Specifies the bucket type. Valid values are \"s3\"and \"nas\".
                uuid: Specifies the unique identifier of the bucket.
                versioning_state: Specifies the versioning state of the bucket. Valid values are \"disabled\", \"enabled\" or \"suspended\". Note that the versioning state cannot be modified to 'disabled' from any other state.
            """

            kwargs = {}
            if allowed is not None:
                kwargs["allowed"] = allowed
            if comment is not None:
                kwargs["comment"] = comment
            if constituents_per_aggregate is not None:
                kwargs["constituents_per_aggregate"] = constituents_per_aggregate
            if logical_used_size is not None:
                kwargs["logical_used_size"] = logical_used_size
            if name is not None:
                kwargs["name"] = name
            if nas_path is not None:
                kwargs["nas_path"] = nas_path
            if role is not None:
                kwargs["role"] = role
            if size is not None:
                kwargs["size"] = size
            if storage_service_level is not None:
                kwargs["storage_service_level"] = storage_service_level
            if type is not None:
                kwargs["type"] = type
            if uuid is not None:
                kwargs["uuid"] = uuid
            if versioning_state is not None:
                kwargs["versioning_state"] = versioning_state

            if hasattr(S3Bucket, "find"):
                resource = S3Bucket.find(
                    **kwargs
                )
            else:
                resource = S3Bucket()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete S3Bucket: %s" % err)


