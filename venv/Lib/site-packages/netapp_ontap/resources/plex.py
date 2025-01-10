r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
The Storage Aggregate Plex API provides relevant state information for each plex in the aggregate.
For each plex, details are provided for the RAID groups in the plex and the disks that make up each RAID group.<br>
## Examples
### Retrieving all aggregates and plexes
The following example shows the response with a list of aggregates and plexes:<br>
# The call:
curl -X GET "https://<mgmt-ip>/api/storage/aggregates/*/plexes" -H "accept: application/json"
# The response:
{
  "records": [
    {
      "aggregate": {
        "uuid": "04b7296e-a302-42a6-a2a9-dda6be054b29",
        "name": "test2"
      },
      "name": "plex0"
    },
    {
      "aggregate": {
        "uuid": "04b7296e-a302-42a6-a2a9-dda6be054b29",
        "name": "test2"
      },
      "name": "plex1",
    },
    {
      "aggregate": {
        "uuid": "66c4b221-65ff-4211-9b58-ada3c6fc41af",
        "name": "test"
      },
      "name": "plex0"
    },
    {
      "aggregate": {
        "uuid": "66c4b221-65ff-4211-9b58-ada3c6fc41af",
        "name": "test"
      },
      "name": "plex1"
    },
    {
      "aggregate": {
        "uuid": "7ee89e48-5d81-4609-9e1b-5d8d0995a886",
        "name": "aggr1",
      },
      "name": "plex0",
    },
    {
      "aggregate": {
        "uuid": "8bb2e3bf-c4f1-4748-9033-ca9231cf1c40",
        "name": "test3",
      },
      "name": "plex0",
    },
    {
      "aggregate": {
        "uuid": "8bb2e3bf-c4f1-4748-9033-ca9231cf1c40",
        "name": "test3",
      },
      "name": "plex1"
    },
    {
      "aggregate": {
        "uuid": "8f13de5c-99cf-4ada-884c-3cc32deb304a",
        "name": "aggr2",
      },
      "name": "plex0"
    }
  ],
  "num_records": 8
}
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Plex

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Plex("*", name="plex0")
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    Plex(
        {
            "aggregate": {
                "name": "test2",
                "uuid": "04b7296e-a302-42a6-a2a9-dda6be054b29",
            },
            "name": "plex0",
        }
    ),
    Plex(
        {
            "aggregate": {
                "name": "test",
                "uuid": "66c4b221-65ff-4211-9b58-ada3c6fc41af",
            },
            "name": "plex0",
        }
    ),
    Plex(
        {
            "aggregate": {
                "name": "aggr1",
                "uuid": "7ee89e48-5d81-4609-9e1b-5d8d0995a886",
            },
            "name": "plex0",
        }
    ),
    Plex(
        {
            "aggregate": {
                "name": "test3",
                "uuid": "8bb2e3bf-c4f1-4748-9033-ca9231cf1c40",
            },
            "name": "plex0",
        }
    ),
    Plex(
        {
            "aggregate": {
                "name": "aggr2",
                "uuid": "8f13de5c-99cf-4ada-884c-3cc32deb304a",
            },
            "name": "plex0",
        }
    ),
]

```
</div>
</div>

### Retrieving the list of plexes in an aggregate
The following example shows the response with the list of plexes in an aggregate:<br>
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Plex

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(Plex.get_collection("19425837-f2fa-4a9f-8f01-712f626c983c")))

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
[Plex({"name": "plex0"}), Plex({"name": "plex4"})]

```
</div>
</div>

### Retrieving a specific plex in an aggregate
The following example shows the response when requesting a specific plex of an aggregate:<br>
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Plex

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Plex("19425837-f2fa-4a9f-8f01-712f626c983c", name="plex0")
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example2_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example2_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example2_result" class="try_it_out_content">
```
Plex(
    {
        "raid_groups": [
            {
                "raid_type": "raid_dp",
                "degraded": False,
                "cache_tier": False,
                "reconstruct": {"active": False},
                "name": "rg0",
                "recomputing_parity": {"active": False},
                "disks": [
                    {
                        "disk": {"name": "1.1.29"},
                        "position": "dparity",
                        "usable_size": 86769664,
                        "state": "normal",
                        "type": "ssd",
                    },
                    {
                        "disk": {"name": "1.1.4"},
                        "position": "parity",
                        "usable_size": 86769664,
                        "state": "normal",
                        "type": "ssd",
                    },
                    {
                        "disk": {"name": "1.1.30"},
                        "position": "data",
                        "usable_size": 86769664,
                        "state": "normal",
                        "type": "ssd",
                    },
                    {
                        "disk": {"name": "1.1.5"},
                        "position": "data",
                        "usable_size": 86769664,
                        "state": "normal",
                        "type": "ssd",
                    },
                    {
                        "disk": {"name": "1.1.31"},
                        "position": "data",
                        "usable_size": 86769664,
                        "state": "normal",
                        "type": "ssd",
                    },
                    {
                        "disk": {"name": "1.1.6"},
                        "position": "data",
                        "usable_size": 86769664,
                        "state": "normal",
                        "type": "ssd",
                    },
                ],
            }
        ],
        "aggregate": {"name": "test1", "uuid": "19425837-f2fa-4a9f-8f01-712f626c983c"},
        "name": "plex0",
        "state": "normal",
        "pool": "pool0",
        "resync": {"active": False},
        "online": True,
    }
)

```
</div>
</div>
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


__all__ = ["Plex", "PlexSchema"]
__pdoc__ = {
    "PlexSchema.resource": False,
    "PlexSchema.opts": False,
    "Plex.plex_show": False,
    "Plex.plex_create": False,
    "Plex.plex_modify": False,
    "Plex.plex_delete": False,
}


class PlexSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the Plex object"""

    aggregate = fields.Nested("netapp_ontap.resources.aggregate.AggregateSchema", data_key="aggregate", unknown=EXCLUDE)
    r""" The aggregate field of the plex."""

    name = fields.Str(
        data_key="name",
    )
    r""" Plex name

Example: plex0"""

    online = fields.Boolean(
        data_key="online",
    )
    r""" Plex is online"""

    pool = fields.Str(
        data_key="pool",
        validate=enum_validation(['pool0', 'pool1']),
    )
    r""" SyncMirror pool assignment

Valid choices:

* pool0
* pool1"""

    raid_groups = fields.List(fields.Nested("netapp_ontap.models.raid_group.RaidGroupSchema", unknown=EXCLUDE), data_key="raid_groups")
    r""" The raid_groups field of the plex."""

    resync = fields.Nested("netapp_ontap.models.plex_resync.PlexResyncSchema", data_key="resync", unknown=EXCLUDE)
    r""" The resync field of the plex."""

    state = fields.Str(
        data_key="state",
        validate=enum_validation(['normal', 'failed', 'out_of_date']),
    )
    r""" Plex state

Valid choices:

* normal
* failed
* out_of_date"""

    @property
    def resource(self):
        return Plex

    gettable_fields = [
        "aggregate.links",
        "aggregate.name",
        "aggregate.uuid",
        "name",
        "online",
        "pool",
        "raid_groups",
        "resync",
        "state",
    ]
    """aggregate.links,aggregate.name,aggregate.uuid,name,online,pool,raid_groups,resync,state,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in Plex.get_collection(fields=field)]
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
            raise NetAppRestError("Plex modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class Plex(Resource):
    """Allows interaction with Plex objects on the host"""

    _schema = PlexSchema
    _path = "/api/storage/aggregates/{aggregate[uuid]}/plexes"
    _keys = ["aggregate.uuid", "name"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the collection of plexes for the specified aggregate.
### Related ONTAP commands
* `storage aggregate plex show`

### Learn more
* [`DOC /storage/aggregates/{aggregate.uuid}/plexes`](#docs-storage-storage_aggregates_{aggregate.uuid}_plexes)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="plex show")
        def plex_show(
            aggregate_uuid,
            name: Choices.define(_get_field_list("name"), cache_choices=True, inexact=True)=None,
            online: Choices.define(_get_field_list("online"), cache_choices=True, inexact=True)=None,
            pool: Choices.define(_get_field_list("pool"), cache_choices=True, inexact=True)=None,
            state: Choices.define(_get_field_list("state"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["name", "online", "pool", "state", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of Plex resources

            Args:
                name: Plex name
                online: Plex is online
                pool: SyncMirror pool assignment
                state: Plex state
            """

            kwargs = {}
            if name is not None:
                kwargs["name"] = name
            if online is not None:
                kwargs["online"] = online
            if pool is not None:
                kwargs["pool"] = pool
            if state is not None:
                kwargs["state"] = state
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return Plex.get_collection(
                aggregate_uuid,
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all Plex resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the collection of plexes for the specified aggregate.
### Related ONTAP commands
* `storage aggregate plex show`

### Learn more
* [`DOC /storage/aggregates/{aggregate.uuid}/plexes`](#docs-storage-storage_aggregates_{aggregate.uuid}_plexes)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the plex specified by the aggregate UUID and plex name.
### Related ONTAP commands
* `storage aggregate plex show`

### Learn more
* [`DOC /storage/aggregates/{aggregate.uuid}/plexes`](#docs-storage-storage_aggregates_{aggregate.uuid}_plexes)"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





