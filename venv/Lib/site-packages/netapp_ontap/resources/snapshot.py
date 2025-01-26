r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
A Snapshot copy is the view of the filesystem as it exists at the time when the Snapshot copy is created. <br/>
In ONTAP, different types of Snapshot copies are supported, such as scheduled Snapshot copies, user requested Snapshot copies, SnapMirror Snapshot copies, and so on. <br/>
ONTAP Snapshot copy APIs allow you to create, modify, delete and retrieve Snapshot copies. <br/>
ONTAP Bulk Snapshot copy APIs allow you to create, modify, delete and retrieve Snapshot copies on multiple volumes in one request. <br/>
## Snapshot copy APIs
The following APIs are used to perform operations related to Snapshot copies.

* POST      /api/storage/volumes/{volume.uuid}/snapshots
* GET       /api/storage/volumes/{volume.uuid}/snapshots
* GET       /api/storage/volumes/{volume.uuid}/snapshots/{uuid}
* PATCH     /api/storage/volumes/{volume.uuid}/snapshots/{uuid}
* DELETE    /api/storage/volumes/{volume.uuid}/snapshots/{uuid}
The following APIs are used to perform bulk operations related to Snapshot copies.

* POST      /api/storage/volumes/*/snapshots
* GET       /api/storage/volumes/*/snapshots
* PATCH     /api/storage/volumes/*/snapshots/{uuid}
* DELETE    /api/storage/volumes/*/snapshots/{uuid}
## Examples
### Creating a Snapshot copy
The POST operation is used to create a Snapshot copy with the specified attributes.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Snapshot("{volume.uuid}")
    resource.name = "snapshot_copy"
    resource.comment = "Store this copy."
    resource.post(hydrate=True)
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
Snapshot(
    {
        "svm": {"name": "vs0", "uuid": "8139f958-3c6e-11e9-a45f-005056bbc848"},
        "name": "snapshot_copy",
        "comment": "Store this copy.",
        "volume": {"name": "v2"},
    }
)

```
</div>
</div>

### Retrieving Snapshot copy attributes
The GET operation is used to retrieve Snapshot copy attributes.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(Snapshot.get_collection("{volume.uuid}")))

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
[
    Snapshot(
        {
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/0353dc05-405f-11e9-acb6-005056bbc848/snapshots/402b6c73-73a0-4e89-a58a-75ee0ab3e8c0"
                }
            },
            "uuid": "402b6c73-73a0-4e89-a58a-75ee0ab3e8c0",
            "name": "hourly.2019-03-13_1305",
        }
    ),
    Snapshot(
        {
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/0353dc05-405f-11e9-acb6-005056bbc848/snapshots/f0dd497f-efe8-44b7-a4f4-bdd3890bc0c8"
                }
            },
            "uuid": "f0dd497f-efe8-44b7-a4f4-bdd3890bc0c8",
            "name": "hourly.2019-03-13_1405",
        }
    ),
    Snapshot(
        {
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/0353dc05-405f-11e9-acb6-005056bbc848/snapshots/02701900-51bd-46b8-9c77-47d9a9e2ce1d"
                }
            },
            "uuid": "02701900-51bd-46b8-9c77-47d9a9e2ce1d",
            "name": "hourly.2019-03-13_1522",
        }
    ),
]

```
</div>
</div>

### Creating bulk Snapshot copies
The POST operation is used to create a Snapshot copy with the same name on multiple volumes in one request.
This operation accepts a volume UUID or volume name and SVM, and a Snapshot copy name.
This operation only supports SnapMirror label attributes to be added to Snapshot copies during creation.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Snapshot("*")
    resource.records = [
        {
            "volume.uuid": "e8815adb-5209-11ec-b4ad-005056bbc3e8",
            "name": "snapshot_copy",
        },
        {
            "volume.uuid": "efda9101-5209-11ec-b4ad-005056bbc3e8",
            "name": "snapshot_copy",
        },
    ]
    resource.post(hydrate=True)
    print(resource)

```
<div class="try_it_out">
<input id="example2_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example2_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example2_result" class="try_it_out_content">
```
Snapshot({})

```
</div>
</div>

### Retrieving Snapshot copy advanced attributes
A collection GET request is used to calculate the amount of Snapshot copy reclaimable space.
When the advanced privilege field 'reclaimable space' is requested, the API returns the amount of reclaimable space for the queried list of Snapshot copies.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(
        list(
            Snapshot.get_collection(
                "{volume.uuid}",
                fields="reclaimable_space",
                name="hourly.2019-03-13_1305|hourly.2019-03-13_1405|hourly.2019-03-13_1522",
            )
        )
    )

```
<div class="try_it_out">
<input id="example3_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example3_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example3_result" class="try_it_out_content">
```
[
    Snapshot(
        {
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/0353dc05-405f-11e9-acb6-005056bbc848/snapshots/402b6c73-73a0-4e89-a58a-75ee0ab3e8c0"
                }
            },
            "uuid": "402b6c73-73a0-4e89-a58a-75ee0ab3e8c0",
            "name": "hourly.2019-03-13_1305",
        }
    ),
    Snapshot(
        {
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/0353dc05-405f-11e9-acb6-005056bbc848/snapshots/f0dd497f-efe8-44b7-a4f4-bdd3890bc0c8"
                }
            },
            "uuid": "f0dd497f-efe8-44b7-a4f4-bdd3890bc0c8",
            "name": "hourly.2019-03-13_1405",
        }
    ),
    Snapshot(
        {
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/0353dc05-405f-11e9-acb6-005056bbc848/snapshots/02701900-51bd-46b8-9c77-47d9a9e2ce1d"
                }
            },
            "uuid": "02701900-51bd-46b8-9c77-47d9a9e2ce1d",
            "name": "hourly.2019-03-13_1522",
        }
    ),
]

```
</div>
</div>

### Retrieving Snapshot copy advanced attributes
A collection GET request is used to calculate the delta between two Snapshot copies.
When the advanced privilege field 'delta' is requested, the API returns the delta between the queried Snapshot copies.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(
        list(
            Snapshot.get_collection(
                "{volume.uuid}",
                fields="delta",
                name="hourly.2022-06-29_1105,hourly.2022-06-29_1205",
            )
        )
    )

```
<div class="try_it_out">
<input id="example4_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example4_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example4_result" class="try_it_out_content">
```
[
    Snapshot(
        {
            "uuid": "52a2247a-7735-4a92-bc3c-e51df1fe502f",
            "name": "hourly.2022-06-29_1105",
            "delta": {"size_consumed": 675840, "time_elapsed": "PT3H27M45S"},
        }
    ),
    Snapshot(
        {
            "uuid": "b399eb34-44fe-4689-9fb5-c8f72162dd77",
            "name": "hourly.2022-06-29_1205",
            "delta": {"size_consumed": 507904, "time_elapsed": "PT2H27M45S"},
        }
    ),
]

```
</div>
</div>

### Retrieving the attributes of a specific Snapshot copy
The GET operation is used to retrieve the attributes of a specific Snapshot copy.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Snapshot(
        "0353dc05-405f-11e9-acb6-005056bbc848",
        uuid="402b6c73-73a0-4e89-a58a-75ee0ab3e8c0",
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example5_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example5_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example5_result" class="try_it_out_content">
```
Snapshot(
    {
        "_links": {
            "self": {
                "href": "/api/storage/volumes/0353dc05-405f-11e9-acb6-005056bbc848/snapshots/402b6c73-73a0-4e89-a58a-75ee0ab3e8c0"
            }
        },
        "uuid": "402b6c73-73a0-4e89-a58a-75ee0ab3e8c0",
        "svm": {
            "name": "vs0",
            "_links": {
                "self": {"href": "/api/svm/svms/8139f958-3c6e-11e9-a45f-005056bbc848"}
            },
            "uuid": "8139f958-3c6e-11e9-a45f-005056bbc848",
        },
        "create_time": "2019-03-13T13:05:00-04:00",
        "name": "hourly.2019-03-13_1305",
        "volume": {
            "name": "v2",
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/0353dc05-405f-11e9-acb6-005056bbc848"
                }
            },
            "uuid": "0353dc05-405f-11e9-acb6-005056bbc848",
        },
        "size": 122880,
    }
)

```
</div>
</div>

### Retrieving the advanced attributes of a specific Snapshot copy
The GET operation is used to retrieve the attributes of a specific Snapshot copy. Snapshot copy reclaimable space can be requested during a GET request.
When the advanced privilege field reclaimable space is requested, the API returns the amount of reclaimable space for the Snapshot copy.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Snapshot(
        "0353dc05-405f-11e9-acb6-005056bbc848",
        uuid="402b6c73-73a0-4e89-a58a-75ee0ab3e8c0",
    )
    resource.get(fields="**")
    print(resource)

```
<div class="try_it_out">
<input id="example6_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example6_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example6_result" class="try_it_out_content">
```
Snapshot(
    {
        "reclaimable_space": 167832,
        "_links": {
            "self": {
                "href": "/api/storage/volumes/0353dc05-405f-11e9-acb6-005056bbc848/snapshots/402b6c73-73a0-4e89-a58a-75ee0ab3e8c0"
            }
        },
        "uuid": "402b6c73-73a0-4e89-a58a-75ee0ab3e8c0",
        "svm": {
            "name": "vs0",
            "_links": {
                "self": {"href": "/api/svm/svms/8139f958-3c6e-11e9-a45f-005056bbc848"}
            },
            "uuid": "8139f958-3c6e-11e9-a45f-005056bbc848",
        },
        "name": "hourly.2019-03-13_1305",
        "volume": {
            "name": "v2",
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/0353dc05-405f-11e9-acb6-005056bbc848"
                }
            },
            "uuid": "0353dc05-405f-11e9-acb6-005056bbc848",
        },
    }
)

```
</div>
</div>

### Retrieving Snapshot copy advanced attributes
A collection GET request is used to calculate the delta between two Snapshot copies.
When the advanced privilege field 'delta' is requested, the API returns the delta between the queried Snapshot copies.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(
        list(
            Snapshot.get_collection(
                "{volume.uuid}",
                fields="delta",
                name="hourly.2022-06-29_1105,hourly.2022-06-29_1205",
            )
        )
    )

```
<div class="try_it_out">
<input id="example4_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example4_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example4_result" class="try_it_out_content">
```
[
    Snapshot(
        {
            "uuid": "52a2247a-7735-4a92-bc3c-e51df1fe502f",
            "name": "hourly.2022-06-29_1105",
            "delta": {"size_consumed": 675840, "time_elapsed": "PT3H27M45S"},
        }
    ),
    Snapshot(
        {
            "uuid": "b399eb34-44fe-4689-9fb5-c8f72162dd77",
            "name": "hourly.2022-06-29_1205",
            "delta": {"size_consumed": 507904, "time_elapsed": "PT2H27M45S"},
        }
    ),
]

```
</div>
</div>

### Retrieving bulk Snapshot copies
The bulk GET operation is used to retrieve Snapshot copy attributes across all volumes.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(Snapshot.get_collection("*")))

```
<div class="try_it_out">
<input id="example8_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example8_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example8_result" class="try_it_out_content">
```
[
    Snapshot(
        {
            "uuid": "3edba912-5507-4535-adce-e12fe5c0e31c",
            "name": "daily.2021-11-18_0010",
            "volume": {"name": "v1", "uuid": "966c285f-47f7-11ec-8407-005056bbc08f"},
        }
    ),
    Snapshot(
        {
            "uuid": "3ad61153-d5ef-495d-8e0e-5c3b8bbaf5e6",
            "name": "hourly.2021-11-18_0705",
            "volume": {"name": "v1", "uuid": "966c285f-47f7-11ec-8407-005056bbc08f"},
        }
    ),
    Snapshot(
        {
            "uuid": "3dd0fa97-65d9-41ea-a99d-5ceb9d2f55c5",
            "name": "daily.2021-11-18_0010",
            "volume": {"name": "v2", "uuid": "99c974e3-47f7-11ec-8407-005056bbc08f"},
        }
    ),
    Snapshot(
        {
            "uuid": "6ca20a52-c342-4753-8865-3693fa9b7e23",
            "name": "hourly.2021-11-18_0705",
            "volume": {"name": "v2", "uuid": "99c974e3-47f7-11ec-8407-005056bbc08f"},
        }
    ),
]

```
</div>
</div>

### Updating a Snapshot copy
The PATCH operation is used to update the specific attributes of a Snapshot copy.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Snapshot(
        "0353dc05-405f-11e9-acb6-005056bbc848",
        uuid="16f7008c-18fd-4a7d-8485-a0e290d9db7f",
    )
    resource.name = "snapshot_copy_new"
    resource.patch()

```

### Updating bulk Snapshot copies
The bulk PATCH operation is used to update the specific attributes of Snapshot copies across volumes in a single request.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Snapshot("*")
    resource.records = [
        {
            "volume.uuid": "e8815adb-5209-11ec-b4ad-005056bbc3e8",
            "svm.uuid": "d0e6def5-5209-11ec-b4ad-005056bbc3e8",
            "uuid": "f9b7714d-1166-410a-b143-874f27969db6",
            "comment": "yay",
        },
        {
            "volume.uuid": "efda9101-5209-11ec-b4ad-005056bbc3e8",
            "svm.uuid": "d0e6def5-5209-11ec-b4ad-005056bbc3e8",
            "uuid": "514c82a7-bff7-48e2-a13c-5337b09ed41e",
            "comment": "yay",
        },
    ]
    resource.patch()

```

### Deleting a Snapshot copy
The DELETE operation is used to delete a Snapshot copy.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Snapshot(
        "0353dc05-405f-11e9-acb6-005056bbc848",
        uuid="16f7008c-18fd-4a7d-8485-a0e290d9db7f",
    )
    resource.delete()

```

### Deleting bulk Snapshot copies
The bulk DELETE operation is used to delete a Snapshot copies across volumes in a single request.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Snapshot

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Snapshot("*")
    resource.delete(
        body={
            "records": [
                {
                    "volume.uuid": "e8815adb-5209-11ec-b4ad-005056bbc3e8",
                    "uuid": "f9b7714d-1166-410a-b143-874f27969db6",
                },
                {
                    "volume.uuid": "efda9101-5209-11ec-b4ad-005056bbc3e8",
                    "uuid": "1d55c97a-25f3-4366-bfa8-9ea75c255469",
                },
            ]
        }
    )

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


__all__ = ["Snapshot", "SnapshotSchema"]
__pdoc__ = {
    "SnapshotSchema.resource": False,
    "SnapshotSchema.opts": False,
    "Snapshot.snapshot_show": False,
    "Snapshot.snapshot_create": False,
    "Snapshot.snapshot_modify": False,
    "Snapshot.snapshot_delete": False,
}


class SnapshotSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the Snapshot object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the snapshot."""

    comment = fields.Str(
        data_key="comment",
    )
    r""" A comment associated with the Snapshot copy. This is an optional attribute for POST or PATCH."""

    create_time = ImpreciseDateTime(
        data_key="create_time",
    )
    r""" Creation time of the Snapshot copy. It is the volume access time when the Snapshot copy was created.

Example: 2019-02-04T19:00:00.000+0000"""

    delta = fields.Nested("netapp_ontap.models.snapshot_delta.SnapshotDeltaSchema", data_key="delta", unknown=EXCLUDE)
    r""" The delta field of the snapshot."""

    expiry_time = ImpreciseDateTime(
        data_key="expiry_time",
    )
    r""" The expiry time for the Snapshot copy. This is an optional attribute for POST or PATCH. Snapshot copies with an expiry time set are not allowed to be deleted until the retention time is reached.

Example: 2019-02-04T19:00:00.000+0000"""

    logical_size = Size(
        data_key="logical_size",
    )
    r""" Size of the logical used file system at the time the Snapshot copy is captured.

Example: 1228800"""

    name = fields.Str(
        data_key="name",
    )
    r""" Snapshot copy. Valid in POST or PATCH.

Example: this_snapshot"""

    owners = fields.List(fields.Str, data_key="owners")
    r""" The owners field of the snapshot."""

    provenance_volume = fields.Nested("netapp_ontap.models.snapshot_provenance_volume.SnapshotProvenanceVolumeSchema", data_key="provenance_volume", unknown=EXCLUDE)
    r""" The provenance_volume field of the snapshot."""

    reclaimable_space = Size(
        data_key="reclaimable_space",
    )
    r""" Space reclaimed when the Snapshot copy is deleted, in bytes."""

    size = Size(
        data_key="size",
    )
    r""" Size of the active file system at the time the Snapshot copy is captured. The actual size of the Snapshot copy also includes those blocks trapped by other Snapshot copies. On a Snapshot copy deletion, the "size" amount of blocks is the maximum number of blocks available. On a Snapshot copy restore, the "afs-used size" value will match the Snapshot copy "size" value.

Example: 122880"""

    snaplock_expiry_time = ImpreciseDateTime(
        data_key="snaplock_expiry_time",
    )
    r""" SnapLock expiry time for the Snapshot copy, if the Snapshot copy is taken on a SnapLock volume. A Snapshot copy is not allowed to be deleted or renamed until the SnapLock ComplianceClock time goes beyond this retention time. This option can be set during Snapshot copy POST and Snapshot copy PATCH on Snapshot copy locking enabled volumes.

Example: 2019-02-04T19:00:00.000+0000"""

    snapmirror_label = fields.Str(
        data_key="snapmirror_label",
    )
    r""" Label for SnapMirror operations"""

    state = fields.Str(
        data_key="state",
        validate=enum_validation(['valid', 'invalid', 'partial']),
    )
    r""" State of the Snapshot copy. There are cases where some Snapshot copies are not complete. In the "partial" state, the Snapshot copy is consistent but exists only on the subset of the constituents that existed prior to the FlexGroup's expansion. Partial Snapshot copies cannot be used for a Snapshot copy restore operation. A Snapshot copy is in an "invalid" state when it is present in some FlexGroup constituents but not in others. At all other times, a Snapshot copy is valid.

Valid choices:

* valid
* invalid
* partial"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the snapshot."""

    uuid = fields.Str(
        data_key="uuid",
    )
    r""" The UUID of the Snapshot copy in the volume that uniquely identifies the Snapshot copy in that volume.

Example: 1cd8a442-86d1-11e0-ae1c-123478563412"""

    version_uuid = fields.Str(
        data_key="version_uuid",
    )
    r""" The 128 bit identifier that uniquely identifies a snapshot and its logical data layout.

Example: 1cd8a442-86d1-11e0-ae1c-123478563412"""

    volume = fields.Nested("netapp_ontap.resources.volume.VolumeSchema", data_key="volume", unknown=EXCLUDE)
    r""" The volume field of the snapshot."""

    @property
    def resource(self):
        return Snapshot

    gettable_fields = [
        "links",
        "comment",
        "create_time",
        "delta",
        "expiry_time",
        "logical_size",
        "name",
        "owners",
        "provenance_volume",
        "reclaimable_space",
        "size",
        "snaplock_expiry_time",
        "snapmirror_label",
        "state",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "uuid",
        "version_uuid",
        "volume.links",
        "volume.name",
        "volume.uuid",
    ]
    """links,comment,create_time,delta,expiry_time,logical_size,name,owners,provenance_volume,reclaimable_space,size,snaplock_expiry_time,snapmirror_label,state,svm.links,svm.name,svm.uuid,uuid,version_uuid,volume.links,volume.name,volume.uuid,"""

    patchable_fields = [
        "comment",
        "delta",
        "expiry_time",
        "name",
        "provenance_volume",
        "reclaimable_space",
        "snaplock_expiry_time",
        "snapmirror_label",
        "svm.name",
        "svm.uuid",
        "volume.name",
        "volume.uuid",
    ]
    """comment,delta,expiry_time,name,provenance_volume,reclaimable_space,snaplock_expiry_time,snapmirror_label,svm.name,svm.uuid,volume.name,volume.uuid,"""

    postable_fields = [
        "comment",
        "delta",
        "expiry_time",
        "name",
        "provenance_volume",
        "reclaimable_space",
        "snaplock_expiry_time",
        "snapmirror_label",
        "svm.name",
        "svm.uuid",
        "volume.name",
        "volume.uuid",
    ]
    """comment,delta,expiry_time,name,provenance_volume,reclaimable_space,snaplock_expiry_time,snapmirror_label,svm.name,svm.uuid,volume.name,volume.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in Snapshot.get_collection(fields=field)]
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
            raise NetAppRestError("Snapshot modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class Snapshot(Resource):
    r""" The Snapshot copy object represents a point in time Snapshot copy of a volume. """

    _schema = SnapshotSchema
    _path = "/api/storage/volumes/{volume[uuid]}/snapshots"
    _keys = ["volume.uuid", "uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves a collection of volume Snapshot copies.
### Expensive properties
There is an added computational cost to retrieving the amount of reclaimable space for Snapshot copies, as the calculation is done on demand based on the list of Snapshot copies provided.
* `reclaimable_space`
* `delta`
### Related ONTAP commands
* `snapshot show`
* `snapshot compute-reclaimable`
* `snapshot show-delta`
### Learn more
* [`DOC /storage/volumes/{volume.uuid}/snapshots`](#docs-storage-storage_volumes_{volume.uuid}_snapshots)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="snapshot show")
        def snapshot_show(
            volume_uuid,
            comment: Choices.define(_get_field_list("comment"), cache_choices=True, inexact=True)=None,
            create_time: Choices.define(_get_field_list("create_time"), cache_choices=True, inexact=True)=None,
            expiry_time: Choices.define(_get_field_list("expiry_time"), cache_choices=True, inexact=True)=None,
            logical_size: Choices.define(_get_field_list("logical_size"), cache_choices=True, inexact=True)=None,
            name: Choices.define(_get_field_list("name"), cache_choices=True, inexact=True)=None,
            owners: Choices.define(_get_field_list("owners"), cache_choices=True, inexact=True)=None,
            reclaimable_space: Choices.define(_get_field_list("reclaimable_space"), cache_choices=True, inexact=True)=None,
            size: Choices.define(_get_field_list("size"), cache_choices=True, inexact=True)=None,
            snaplock_expiry_time: Choices.define(_get_field_list("snaplock_expiry_time"), cache_choices=True, inexact=True)=None,
            snapmirror_label: Choices.define(_get_field_list("snapmirror_label"), cache_choices=True, inexact=True)=None,
            state: Choices.define(_get_field_list("state"), cache_choices=True, inexact=True)=None,
            uuid: Choices.define(_get_field_list("uuid"), cache_choices=True, inexact=True)=None,
            version_uuid: Choices.define(_get_field_list("version_uuid"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["comment", "create_time", "expiry_time", "logical_size", "name", "owners", "reclaimable_space", "size", "snaplock_expiry_time", "snapmirror_label", "state", "uuid", "version_uuid", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of Snapshot resources

            Args:
                comment: A comment associated with the Snapshot copy. This is an optional attribute for POST or PATCH.
                create_time: Creation time of the Snapshot copy. It is the volume access time when the Snapshot copy was created.
                expiry_time: The expiry time for the Snapshot copy. This is an optional attribute for POST or PATCH. Snapshot copies with an expiry time set are not allowed to be deleted until the retention time is reached.
                logical_size: Size of the logical used file system at the time the Snapshot copy is captured.
                name: Snapshot copy. Valid in POST or PATCH.
                owners: 
                reclaimable_space: Space reclaimed when the Snapshot copy is deleted, in bytes.
                size: Size of the active file system at the time the Snapshot copy is captured. The actual size of the Snapshot copy also includes those blocks trapped by other Snapshot copies. On a Snapshot copy deletion, the \"size\" amount of blocks is the maximum number of blocks available. On a Snapshot copy restore, the \"afs-used size\" value will match the Snapshot copy \"size\" value.
                snaplock_expiry_time: SnapLock expiry time for the Snapshot copy, if the Snapshot copy is taken on a SnapLock volume. A Snapshot copy is not allowed to be deleted or renamed until the SnapLock ComplianceClock time goes beyond this retention time. This option can be set during Snapshot copy POST and Snapshot copy PATCH on Snapshot copy locking enabled volumes.
                snapmirror_label: Label for SnapMirror operations
                state: State of the Snapshot copy. There are cases where some Snapshot copies are not complete. In the \"partial\" state, the Snapshot copy is consistent but exists only on the subset of the constituents that existed prior to the FlexGroup's expansion. Partial Snapshot copies cannot be used for a Snapshot copy restore operation. A Snapshot copy is in an \"invalid\" state when it is present in some FlexGroup constituents but not in others. At all other times, a Snapshot copy is valid.
                uuid: The UUID of the Snapshot copy in the volume that uniquely identifies the Snapshot copy in that volume.
                version_uuid: The 128 bit identifier that uniquely identifies a snapshot and its logical data layout.
            """

            kwargs = {}
            if comment is not None:
                kwargs["comment"] = comment
            if create_time is not None:
                kwargs["create_time"] = create_time
            if expiry_time is not None:
                kwargs["expiry_time"] = expiry_time
            if logical_size is not None:
                kwargs["logical_size"] = logical_size
            if name is not None:
                kwargs["name"] = name
            if owners is not None:
                kwargs["owners"] = owners
            if reclaimable_space is not None:
                kwargs["reclaimable_space"] = reclaimable_space
            if size is not None:
                kwargs["size"] = size
            if snaplock_expiry_time is not None:
                kwargs["snaplock_expiry_time"] = snaplock_expiry_time
            if snapmirror_label is not None:
                kwargs["snapmirror_label"] = snapmirror_label
            if state is not None:
                kwargs["state"] = state
            if uuid is not None:
                kwargs["uuid"] = uuid
            if version_uuid is not None:
                kwargs["version_uuid"] = version_uuid
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return Snapshot.get_collection(
                volume_uuid,
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all Snapshot resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["Snapshot"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a Volume Snapshot copy.
### Related ONTAP commands
* `snapshot modify`
* `snapshot rename`
### Learn more
* [`DOC /storage/volumes/{volume.uuid}/snapshots`](#docs-storage-storage_volumes_{volume.uuid}_snapshots)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["Snapshot"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["Snapshot"], NetAppResponse]:
        r"""Creates a volume Snapshot copy.
### Required properties
* `name` - Name of the Snapshot copy to be created.
### Recommended optional properties
* `comment` - Comment associated with the Snapshot copy.
* `expiry_time` - Snapshot copies with an expiry time set are not allowed to be deleted until the retention time is reached.
* `snapmirror_label` - Label for SnapMirror operations.
* `snaplock_expiry_time` - Expiry time for Snapshot copy locking enabled volumes.
### Related ONTAP commands
* `snapshot create`
### Learn more
* [`DOC /storage/volumes/{volume.uuid}/snapshots`](#docs-storage-storage_volumes_{volume.uuid}_snapshots)
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
        records: Iterable["Snapshot"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a Volume Snapshot copy.
### Related ONTAP commands
* `snapshot delete`
### Learn more
* [`DOC /storage/volumes/{volume.uuid}/snapshots`](#docs-storage-storage_volumes_{volume.uuid}_snapshots)
"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves a collection of volume Snapshot copies.
### Expensive properties
There is an added computational cost to retrieving the amount of reclaimable space for Snapshot copies, as the calculation is done on demand based on the list of Snapshot copies provided.
* `reclaimable_space`
* `delta`
### Related ONTAP commands
* `snapshot show`
* `snapshot compute-reclaimable`
* `snapshot show-delta`
### Learn more
* [`DOC /storage/volumes/{volume.uuid}/snapshots`](#docs-storage-storage_volumes_{volume.uuid}_snapshots)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves details of a specific volume Snapshot copy.
### Related ONTAP commands
* `snapshot show`
### Learn more
* [`DOC /storage/volumes/{volume.uuid}/snapshots`](#docs-storage-storage_volumes_{volume.uuid}_snapshots)
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
        r"""Creates a volume Snapshot copy.
### Required properties
* `name` - Name of the Snapshot copy to be created.
### Recommended optional properties
* `comment` - Comment associated with the Snapshot copy.
* `expiry_time` - Snapshot copies with an expiry time set are not allowed to be deleted until the retention time is reached.
* `snapmirror_label` - Label for SnapMirror operations.
* `snaplock_expiry_time` - Expiry time for Snapshot copy locking enabled volumes.
### Related ONTAP commands
* `snapshot create`
### Learn more
* [`DOC /storage/volumes/{volume.uuid}/snapshots`](#docs-storage-storage_volumes_{volume.uuid}_snapshots)
"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="snapshot create")
        async def snapshot_create(
            volume_uuid,
            links: dict = None,
            comment: str = None,
            create_time: datetime = None,
            delta: dict = None,
            expiry_time: datetime = None,
            logical_size: Size = None,
            name: str = None,
            owners: dict = None,
            provenance_volume: dict = None,
            reclaimable_space: Size = None,
            size: Size = None,
            snaplock_expiry_time: datetime = None,
            snapmirror_label: str = None,
            state: str = None,
            svm: dict = None,
            uuid: str = None,
            version_uuid: str = None,
            volume: dict = None,
        ) -> ResourceTable:
            """Create an instance of a Snapshot resource

            Args:
                links: 
                comment: A comment associated with the Snapshot copy. This is an optional attribute for POST or PATCH.
                create_time: Creation time of the Snapshot copy. It is the volume access time when the Snapshot copy was created.
                delta: 
                expiry_time: The expiry time for the Snapshot copy. This is an optional attribute for POST or PATCH. Snapshot copies with an expiry time set are not allowed to be deleted until the retention time is reached.
                logical_size: Size of the logical used file system at the time the Snapshot copy is captured.
                name: Snapshot copy. Valid in POST or PATCH.
                owners: 
                provenance_volume: 
                reclaimable_space: Space reclaimed when the Snapshot copy is deleted, in bytes.
                size: Size of the active file system at the time the Snapshot copy is captured. The actual size of the Snapshot copy also includes those blocks trapped by other Snapshot copies. On a Snapshot copy deletion, the \"size\" amount of blocks is the maximum number of blocks available. On a Snapshot copy restore, the \"afs-used size\" value will match the Snapshot copy \"size\" value.
                snaplock_expiry_time: SnapLock expiry time for the Snapshot copy, if the Snapshot copy is taken on a SnapLock volume. A Snapshot copy is not allowed to be deleted or renamed until the SnapLock ComplianceClock time goes beyond this retention time. This option can be set during Snapshot copy POST and Snapshot copy PATCH on Snapshot copy locking enabled volumes.
                snapmirror_label: Label for SnapMirror operations
                state: State of the Snapshot copy. There are cases where some Snapshot copies are not complete. In the \"partial\" state, the Snapshot copy is consistent but exists only on the subset of the constituents that existed prior to the FlexGroup's expansion. Partial Snapshot copies cannot be used for a Snapshot copy restore operation. A Snapshot copy is in an \"invalid\" state when it is present in some FlexGroup constituents but not in others. At all other times, a Snapshot copy is valid.
                svm: 
                uuid: The UUID of the Snapshot copy in the volume that uniquely identifies the Snapshot copy in that volume.
                version_uuid: The 128 bit identifier that uniquely identifies a snapshot and its logical data layout.
                volume: 
            """

            kwargs = {}
            if links is not None:
                kwargs["links"] = links
            if comment is not None:
                kwargs["comment"] = comment
            if create_time is not None:
                kwargs["create_time"] = create_time
            if delta is not None:
                kwargs["delta"] = delta
            if expiry_time is not None:
                kwargs["expiry_time"] = expiry_time
            if logical_size is not None:
                kwargs["logical_size"] = logical_size
            if name is not None:
                kwargs["name"] = name
            if owners is not None:
                kwargs["owners"] = owners
            if provenance_volume is not None:
                kwargs["provenance_volume"] = provenance_volume
            if reclaimable_space is not None:
                kwargs["reclaimable_space"] = reclaimable_space
            if size is not None:
                kwargs["size"] = size
            if snaplock_expiry_time is not None:
                kwargs["snaplock_expiry_time"] = snaplock_expiry_time
            if snapmirror_label is not None:
                kwargs["snapmirror_label"] = snapmirror_label
            if state is not None:
                kwargs["state"] = state
            if svm is not None:
                kwargs["svm"] = svm
            if uuid is not None:
                kwargs["uuid"] = uuid
            if version_uuid is not None:
                kwargs["version_uuid"] = version_uuid
            if volume is not None:
                kwargs["volume"] = volume

            resource = Snapshot(
                volume_uuid,
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create Snapshot: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a Volume Snapshot copy.
### Related ONTAP commands
* `snapshot modify`
* `snapshot rename`
### Learn more
* [`DOC /storage/volumes/{volume.uuid}/snapshots`](#docs-storage-storage_volumes_{volume.uuid}_snapshots)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="snapshot modify")
        async def snapshot_modify(
            volume_uuid,
            comment: str = None,
            query_comment: str = None,
            create_time: datetime = None,
            query_create_time: datetime = None,
            expiry_time: datetime = None,
            query_expiry_time: datetime = None,
            logical_size: Size = None,
            query_logical_size: Size = None,
            name: str = None,
            query_name: str = None,
            owners: dict = None,
            query_owners: dict = None,
            reclaimable_space: Size = None,
            query_reclaimable_space: Size = None,
            size: Size = None,
            query_size: Size = None,
            snaplock_expiry_time: datetime = None,
            query_snaplock_expiry_time: datetime = None,
            snapmirror_label: str = None,
            query_snapmirror_label: str = None,
            state: str = None,
            query_state: str = None,
            uuid: str = None,
            query_uuid: str = None,
            version_uuid: str = None,
            query_version_uuid: str = None,
        ) -> ResourceTable:
            """Modify an instance of a Snapshot resource

            Args:
                comment: A comment associated with the Snapshot copy. This is an optional attribute for POST or PATCH.
                query_comment: A comment associated with the Snapshot copy. This is an optional attribute for POST or PATCH.
                create_time: Creation time of the Snapshot copy. It is the volume access time when the Snapshot copy was created.
                query_create_time: Creation time of the Snapshot copy. It is the volume access time when the Snapshot copy was created.
                expiry_time: The expiry time for the Snapshot copy. This is an optional attribute for POST or PATCH. Snapshot copies with an expiry time set are not allowed to be deleted until the retention time is reached.
                query_expiry_time: The expiry time for the Snapshot copy. This is an optional attribute for POST or PATCH. Snapshot copies with an expiry time set are not allowed to be deleted until the retention time is reached.
                logical_size: Size of the logical used file system at the time the Snapshot copy is captured.
                query_logical_size: Size of the logical used file system at the time the Snapshot copy is captured.
                name: Snapshot copy. Valid in POST or PATCH.
                query_name: Snapshot copy. Valid in POST or PATCH.
                owners: 
                query_owners: 
                reclaimable_space: Space reclaimed when the Snapshot copy is deleted, in bytes.
                query_reclaimable_space: Space reclaimed when the Snapshot copy is deleted, in bytes.
                size: Size of the active file system at the time the Snapshot copy is captured. The actual size of the Snapshot copy also includes those blocks trapped by other Snapshot copies. On a Snapshot copy deletion, the \"size\" amount of blocks is the maximum number of blocks available. On a Snapshot copy restore, the \"afs-used size\" value will match the Snapshot copy \"size\" value.
                query_size: Size of the active file system at the time the Snapshot copy is captured. The actual size of the Snapshot copy also includes those blocks trapped by other Snapshot copies. On a Snapshot copy deletion, the \"size\" amount of blocks is the maximum number of blocks available. On a Snapshot copy restore, the \"afs-used size\" value will match the Snapshot copy \"size\" value.
                snaplock_expiry_time: SnapLock expiry time for the Snapshot copy, if the Snapshot copy is taken on a SnapLock volume. A Snapshot copy is not allowed to be deleted or renamed until the SnapLock ComplianceClock time goes beyond this retention time. This option can be set during Snapshot copy POST and Snapshot copy PATCH on Snapshot copy locking enabled volumes.
                query_snaplock_expiry_time: SnapLock expiry time for the Snapshot copy, if the Snapshot copy is taken on a SnapLock volume. A Snapshot copy is not allowed to be deleted or renamed until the SnapLock ComplianceClock time goes beyond this retention time. This option can be set during Snapshot copy POST and Snapshot copy PATCH on Snapshot copy locking enabled volumes.
                snapmirror_label: Label for SnapMirror operations
                query_snapmirror_label: Label for SnapMirror operations
                state: State of the Snapshot copy. There are cases where some Snapshot copies are not complete. In the \"partial\" state, the Snapshot copy is consistent but exists only on the subset of the constituents that existed prior to the FlexGroup's expansion. Partial Snapshot copies cannot be used for a Snapshot copy restore operation. A Snapshot copy is in an \"invalid\" state when it is present in some FlexGroup constituents but not in others. At all other times, a Snapshot copy is valid.
                query_state: State of the Snapshot copy. There are cases where some Snapshot copies are not complete. In the \"partial\" state, the Snapshot copy is consistent but exists only on the subset of the constituents that existed prior to the FlexGroup's expansion. Partial Snapshot copies cannot be used for a Snapshot copy restore operation. A Snapshot copy is in an \"invalid\" state when it is present in some FlexGroup constituents but not in others. At all other times, a Snapshot copy is valid.
                uuid: The UUID of the Snapshot copy in the volume that uniquely identifies the Snapshot copy in that volume.
                query_uuid: The UUID of the Snapshot copy in the volume that uniquely identifies the Snapshot copy in that volume.
                version_uuid: The 128 bit identifier that uniquely identifies a snapshot and its logical data layout.
                query_version_uuid: The 128 bit identifier that uniquely identifies a snapshot and its logical data layout.
            """

            kwargs = {}
            changes = {}
            if query_comment is not None:
                kwargs["comment"] = query_comment
            if query_create_time is not None:
                kwargs["create_time"] = query_create_time
            if query_expiry_time is not None:
                kwargs["expiry_time"] = query_expiry_time
            if query_logical_size is not None:
                kwargs["logical_size"] = query_logical_size
            if query_name is not None:
                kwargs["name"] = query_name
            if query_owners is not None:
                kwargs["owners"] = query_owners
            if query_reclaimable_space is not None:
                kwargs["reclaimable_space"] = query_reclaimable_space
            if query_size is not None:
                kwargs["size"] = query_size
            if query_snaplock_expiry_time is not None:
                kwargs["snaplock_expiry_time"] = query_snaplock_expiry_time
            if query_snapmirror_label is not None:
                kwargs["snapmirror_label"] = query_snapmirror_label
            if query_state is not None:
                kwargs["state"] = query_state
            if query_uuid is not None:
                kwargs["uuid"] = query_uuid
            if query_version_uuid is not None:
                kwargs["version_uuid"] = query_version_uuid

            if comment is not None:
                changes["comment"] = comment
            if create_time is not None:
                changes["create_time"] = create_time
            if expiry_time is not None:
                changes["expiry_time"] = expiry_time
            if logical_size is not None:
                changes["logical_size"] = logical_size
            if name is not None:
                changes["name"] = name
            if owners is not None:
                changes["owners"] = owners
            if reclaimable_space is not None:
                changes["reclaimable_space"] = reclaimable_space
            if size is not None:
                changes["size"] = size
            if snaplock_expiry_time is not None:
                changes["snaplock_expiry_time"] = snaplock_expiry_time
            if snapmirror_label is not None:
                changes["snapmirror_label"] = snapmirror_label
            if state is not None:
                changes["state"] = state
            if uuid is not None:
                changes["uuid"] = uuid
            if version_uuid is not None:
                changes["version_uuid"] = version_uuid

            if hasattr(Snapshot, "find"):
                resource = Snapshot.find(
                    volume_uuid,
                    **kwargs
                )
            else:
                resource = Snapshot(volume_uuid,)
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify Snapshot: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a Volume Snapshot copy.
### Related ONTAP commands
* `snapshot delete`
### Learn more
* [`DOC /storage/volumes/{volume.uuid}/snapshots`](#docs-storage-storage_volumes_{volume.uuid}_snapshots)
"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="snapshot delete")
        async def snapshot_delete(
            volume_uuid,
            comment: str = None,
            create_time: datetime = None,
            expiry_time: datetime = None,
            logical_size: Size = None,
            name: str = None,
            owners: dict = None,
            reclaimable_space: Size = None,
            size: Size = None,
            snaplock_expiry_time: datetime = None,
            snapmirror_label: str = None,
            state: str = None,
            uuid: str = None,
            version_uuid: str = None,
        ) -> None:
            """Delete an instance of a Snapshot resource

            Args:
                comment: A comment associated with the Snapshot copy. This is an optional attribute for POST or PATCH.
                create_time: Creation time of the Snapshot copy. It is the volume access time when the Snapshot copy was created.
                expiry_time: The expiry time for the Snapshot copy. This is an optional attribute for POST or PATCH. Snapshot copies with an expiry time set are not allowed to be deleted until the retention time is reached.
                logical_size: Size of the logical used file system at the time the Snapshot copy is captured.
                name: Snapshot copy. Valid in POST or PATCH.
                owners: 
                reclaimable_space: Space reclaimed when the Snapshot copy is deleted, in bytes.
                size: Size of the active file system at the time the Snapshot copy is captured. The actual size of the Snapshot copy also includes those blocks trapped by other Snapshot copies. On a Snapshot copy deletion, the \"size\" amount of blocks is the maximum number of blocks available. On a Snapshot copy restore, the \"afs-used size\" value will match the Snapshot copy \"size\" value.
                snaplock_expiry_time: SnapLock expiry time for the Snapshot copy, if the Snapshot copy is taken on a SnapLock volume. A Snapshot copy is not allowed to be deleted or renamed until the SnapLock ComplianceClock time goes beyond this retention time. This option can be set during Snapshot copy POST and Snapshot copy PATCH on Snapshot copy locking enabled volumes.
                snapmirror_label: Label for SnapMirror operations
                state: State of the Snapshot copy. There are cases where some Snapshot copies are not complete. In the \"partial\" state, the Snapshot copy is consistent but exists only on the subset of the constituents that existed prior to the FlexGroup's expansion. Partial Snapshot copies cannot be used for a Snapshot copy restore operation. A Snapshot copy is in an \"invalid\" state when it is present in some FlexGroup constituents but not in others. At all other times, a Snapshot copy is valid.
                uuid: The UUID of the Snapshot copy in the volume that uniquely identifies the Snapshot copy in that volume.
                version_uuid: The 128 bit identifier that uniquely identifies a snapshot and its logical data layout.
            """

            kwargs = {}
            if comment is not None:
                kwargs["comment"] = comment
            if create_time is not None:
                kwargs["create_time"] = create_time
            if expiry_time is not None:
                kwargs["expiry_time"] = expiry_time
            if logical_size is not None:
                kwargs["logical_size"] = logical_size
            if name is not None:
                kwargs["name"] = name
            if owners is not None:
                kwargs["owners"] = owners
            if reclaimable_space is not None:
                kwargs["reclaimable_space"] = reclaimable_space
            if size is not None:
                kwargs["size"] = size
            if snaplock_expiry_time is not None:
                kwargs["snaplock_expiry_time"] = snaplock_expiry_time
            if snapmirror_label is not None:
                kwargs["snapmirror_label"] = snapmirror_label
            if state is not None:
                kwargs["state"] = state
            if uuid is not None:
                kwargs["uuid"] = uuid
            if version_uuid is not None:
                kwargs["version_uuid"] = version_uuid

            if hasattr(Snapshot, "find"):
                resource = Snapshot.find(
                    volume_uuid,
                    **kwargs
                )
            else:
                resource = Snapshot(volume_uuid,)
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete Snapshot: %s" % err)


