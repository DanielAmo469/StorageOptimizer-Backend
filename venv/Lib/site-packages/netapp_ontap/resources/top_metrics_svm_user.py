r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
You can use this API to retrieve a list of users with the most IO activity for FlexVol and FlexGroup volumes belonging to a specified SVM. To obtain this list, only the volumes which have the activity tracking feature enabled are considered. </br>
This API is used to provide insight into IO activity and supports ordering by IO activity types, namely `iops` and `throughput` metrics. Use the `top_metric` parameter to specify which type of IO activity to filter for. This API supports returning only one IO activity type per request.</br>
## Enabling and disabling activity tracking feature
The following APIs can be used to enable, disable, and retrieve the activity tracking state for a FlexVol or a FlexGroup volume.

* PATCH  /api/storage/volumes/{uuid} -d '{"activity_tracking.state":"on"}'
* PATCH  /api/storage/volumes/{uuid} -d '{"activity_tracking.state":"off"}'
* GET    /api/storage/volumes/{uuid}/?fields=activity_tracking
## Excluded volumes list
Optionally, the API returns an excluded list of activity tracking-enabled volumes, which were not accounted for when obtaining the list of clients with the most IO activity for the SVM. This excluded list contains both the volume information and the reason for exclusion.
## Failure to return list of users with most IO activity
The API can sometimes fail to return the list of users with the most IO activity, due to the following reasons.

* The volumes belonging to the SVM do not have the activity tracking feature enabled.
* The volumes belonging to the SVM do not have read/write traffic.
* The read traffic is served by the NFS/CIFS client filesystem cache.
* On rare occasions, the incoming traffic pattern is not suitable to obtain the list of users with the most IO activity.
## Failure to return the usernames
The API can sometimes fail to obtain the usernames for the list of userid entries, due to internal transient errors.
In such cases, instead of the username, the API will return "{<user-id>}" for every user entry.
## Retrieve a list of the users with the most IO activity
For a report on the users with the most IO activity returned in descending order, specify the IO activity type you want to filter for by passing the `iops` or `throughput` property into the top_metric parameter. If the IO activity type is not specified, by default the API returns a list of the users with the greatest number of the average read operations per second. The maximum number of users returned by the API for an IO activity type is 25.

* GET   /api/svm/svms/{svm.uuid}/top-metrics/users
## Examples
### Retrieving a list of the users with the greatest average number of read bytes received per second:
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import TopMetricsSvmUser

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(
        list(
            TopMetricsSvmUser.get_collection("{svm.uuid}", top_metric="throughput.read")
        )
    )

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    TopMetricsSvmUser(
        {
            "user_name": "user1",
            "user_id": "S-1-5-21-256008430-3394229847-3930036330-1001",
            "svm": {"name": "vs1"},
            "iops": {
                "write": 1495,
                "error": {"upper_bound": 1505, "lower_bound": 1495},
            },
            "volumes": [
                {
                    "name": "fv1",
                    "_links": {
                        "self": {
                            "href": "/api/storage/volumes/73b293df-e9d7-46cc-a9ce-2df8e52ef864"
                        }
                    },
                    "uuid": "73b293df-e9d7-46cc-a9ce-2df8e52ef864",
                },
                {
                    "name": "fv2",
                    "_links": {
                        "self": {
                            "href": "/api/storage/volumes/2ea74c3e-d5ca-11eb-8fbb-005056ac0f33"
                        }
                    },
                    "uuid": "2ea74c3e-d5ca-11eb-8fbb-005056ac0f33",
                },
                {
                    "name": "fv4",
                    "_links": {
                        "self": {
                            "href": "/api/storage/volumes/5bbfc226-3fd8-42c9-a651-fa6167c2cf84"
                        }
                    },
                    "uuid": "5bbfc226-3fd8-42c9-a651-fa6167c2cf84",
                },
            ],
        }
    ),
    TopMetricsSvmUser(
        {
            "user_name": "user2",
            "user_id": "S-1-5-21-256008430-3394229847-3930036330-1002",
            "svm": {"name": "vs1"},
            "iops": {
                "write": 1022,
                "error": {"upper_bound": 1032, "lower_bound": 1022},
            },
            "volumes": [
                {
                    "name": "fv2",
                    "_links": {
                        "self": {
                            "href": "/api/storage/volumes/2ea74c3e-d5ca-11eb-8fbb-005056ac0f33"
                        }
                    },
                    "uuid": "2ea74c3e-d5ca-11eb-8fbb-005056ac0f33",
                },
                {
                    "name": "fv3",
                    "_links": {
                        "self": {
                            "href": "/api/storage/volumes/1ca74c3e-d5ca-11eb-8fbb-005056ac0f88"
                        }
                    },
                    "uuid": "1ca74c3e-d5ca-11eb-8fbb-005056ac0f88",
                },
            ],
        }
    ),
    TopMetricsSvmUser(
        {
            "user_name": "user3",
            "user_id": "S-1-5-21-256008430-3394229847-3930036330-1003",
            "svm": {"name": "vs1"},
            "iops": {"write": 345, "error": {"upper_bound": 355, "lower_bound": 345}},
            "volumes": [
                {
                    "name": "fv3",
                    "_links": {
                        "self": {
                            "href": "/api/storage/volumes/1ca74c3e-d5ca-11eb-8fbb-005056ac0f88"
                        }
                    },
                    "uuid": "1ca74c3e-d5ca-11eb-8fbb-005056ac0f88",
                }
            ],
        }
    ),
    TopMetricsSvmUser(
        {
            "user_name": "user4",
            "user_id": "1988",
            "svm": {"name": "vs1"},
            "iops": {"write": 235, "error": {"upper_bound": 245, "lower_bound": 235}},
            "volumes": [
                {
                    "name": "fv4",
                    "_links": {
                        "self": {
                            "href": "/api/storage/volumes/5bbfc226-3fd8-42c9-a651-fa6167c2cf84"
                        }
                    },
                    "uuid": "5bbfc226-3fd8-42c9-a651-fa6167c2cf84",
                }
            ],
        }
    ),
    TopMetricsSvmUser(
        {
            "user_name": "user5",
            "user_id": "S-1-5-21-256008430-3394229847-3930036330-1005",
            "svm": {"name": "vs1"},
            "iops": {"write": 235, "error": {"upper_bound": 245, "lower_bound": 235}},
            "volumes": [
                {
                    "name": "fv5",
                    "_links": {
                        "self": {
                            "href": "/api/storage/volumes/5bbfc227-3fd8-42c9-a651-fa6167c2cf85"
                        }
                    },
                    "uuid": "5bbfc227-3fd8-42c9-a651-fa6167c2cf85",
                }
            ],
        }
    ),
]

```
</div>
</div>

---
### Example showing the behavior of the API where there is no read/write traffic:
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import TopMetricsSvmUser

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(
        list(
            TopMetricsSvmUser.get_collection(
                "{svm.uuid}", top_metric="throughput.write"
            )
        )
    )

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
[]

```
</div>
</div>

---"""

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


__all__ = ["TopMetricsSvmUser", "TopMetricsSvmUserSchema"]
__pdoc__ = {
    "TopMetricsSvmUserSchema.resource": False,
    "TopMetricsSvmUserSchema.opts": False,
    "TopMetricsSvmUser.top_metrics_svm_user_show": False,
    "TopMetricsSvmUser.top_metrics_svm_user_create": False,
    "TopMetricsSvmUser.top_metrics_svm_user_modify": False,
    "TopMetricsSvmUser.top_metrics_svm_user_delete": False,
}


class TopMetricsSvmUserSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TopMetricsSvmUser object"""

    iops = fields.Nested("netapp_ontap.models.top_metrics_svm_user_iops.TopMetricsSvmUserIopsSchema", data_key="iops", unknown=EXCLUDE)
    r""" The iops field of the top_metrics_svm_user."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the top_metrics_svm_user."""

    throughput = fields.Nested("netapp_ontap.models.top_metrics_svm_user_throughput.TopMetricsSvmUserThroughputSchema", data_key="throughput", unknown=EXCLUDE)
    r""" The throughput field of the top_metrics_svm_user."""

    user_id = fields.Str(
        data_key="user_id",
    )
    r""" User ID of the user.

Example: S-1-5-21-256008430-3394229847-3930036330-1001"""

    user_name = fields.Str(
        data_key="user_name",
    )
    r""" Name of the user.

Example: James"""

    volumes = fields.List(fields.Nested("netapp_ontap.resources.volume.VolumeSchema", unknown=EXCLUDE), data_key="volumes")
    r""" List of volumes where the user is generating traffic."""

    @property
    def resource(self):
        return TopMetricsSvmUser

    gettable_fields = [
        "iops",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "throughput",
        "user_id",
        "user_name",
        "volumes.links",
        "volumes.name",
        "volumes.uuid",
    ]
    """iops,svm.links,svm.name,svm.uuid,throughput,user_id,user_name,volumes.links,volumes.name,volumes.uuid,"""

    patchable_fields = [
        "iops",
        "svm.name",
        "svm.uuid",
        "throughput",
        "volumes.name",
        "volumes.uuid",
    ]
    """iops,svm.name,svm.uuid,throughput,volumes.name,volumes.uuid,"""

    postable_fields = [
        "iops",
        "svm.name",
        "svm.uuid",
        "throughput",
        "volumes.name",
        "volumes.uuid",
    ]
    """iops,svm.name,svm.uuid,throughput,volumes.name,volumes.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in TopMetricsSvmUser.get_collection(fields=field)]
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
            raise NetAppRestError("TopMetricsSvmUser modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class TopMetricsSvmUser(Resource):
    r""" Aggregated information about a user's IO activity at a SVM scope. """

    _schema = TopMetricsSvmUserSchema
    _path = "/api/svm/svms/{svm[uuid]}/top-metrics/users"
    _keys = ["svm.uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves a list of users with the most IO activity.
### Learn more
* [`DOC /svm/svms/{svm.uuid}/top-metrics/users`](#docs-svm-svm_svms_{svm.uuid}_top-metrics_users)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="top metrics svm user show")
        def top_metrics_svm_user_show(
            svm_uuid,
            user_id: Choices.define(_get_field_list("user_id"), cache_choices=True, inexact=True)=None,
            user_name: Choices.define(_get_field_list("user_name"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["user_id", "user_name", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of TopMetricsSvmUser resources

            Args:
                user_id: User ID of the user.
                user_name: Name of the user.
            """

            kwargs = {}
            if user_id is not None:
                kwargs["user_id"] = user_id
            if user_name is not None:
                kwargs["user_name"] = user_name
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return TopMetricsSvmUser.get_collection(
                svm_uuid,
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all TopMetricsSvmUser resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves a list of users with the most IO activity.
### Learn more
* [`DOC /svm/svms/{svm.uuid}/top-metrics/users`](#docs-svm-svm_svms_{svm.uuid}_top-metrics_users)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)






