r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
You can use this API to start a file move operation between two FlexVol volumes or within a FlexGroup volume, and view the status of all on-going file move operations in the cluster.

* GET       /api/storage/file/moves
* GET       /api/storage/file/moves/{node.uuid}/{uuid}/{index}
* POST      /api/storage/file/moves
## Examples
### Moving two files from one FlexVol volume to the other FlexVol volume
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileMove

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileMove()
    resource.files_to_move = {
        "sources": [
            {"svm": {"name": "vs0"}, "volume": {"name": "fv1"}, "path": "dir1/f1.txt"},
            {"svm": {"name": "vs0"}, "volume": {"name": "fv1"}, "path": "dir1/f2.txt"},
        ],
        "destinations": [
            {"svm": {"name": "vs0"}, "volume": {"name": "fv2"}, "path": "dir2/f1.txt"},
            {"svm": {"name": "vs0"}, "volume": {"name": "fv2"}, "path": "dir2/f2.txt"},
        ],
    }
    resource.post(hydrate=True)
    print(resource)

```

### Moving two files from one FlexVol volume to the other FlexVol volume (only specifying the destination directory)
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileMove

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileMove()
    resource.files_to_move = {
        "sources": [
            {"svm": {"name": "vs0"}, "volume": {"name": "fv1"}, "path": "dir1/f3.txt"},
            {"svm": {"name": "vs0"}, "volume": {"name": "fv1"}, "path": "dir1/f4.txt"},
        ],
        "destinations": [
            {"svm": {"name": "vs0"}, "volume": {"name": "fv2"}, "path": "dir2/"}
        ],
    }
    resource.post(hydrate=True)
    print(resource)

```

### Moving multiple files from one FlexVol volume to the other FlexVol volume and providing a source reference file
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileMove

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileMove()
    resource.files_to_move = {
        "sources": [
            {"svm": {"name": "vs0"}, "volume": {"name": "fv1"}, "path": "dir1/f5.txt"},
            {"svm": {"name": "vs0"}, "volume": {"name": "fv1"}, "path": "dir1/f6.txt"},
            {"svm": {"name": "vs0"}, "volume": {"name": "fv1"}, "path": "dir1/f7.txt"},
        ],
        "destinations": [
            {"svm": {"name": "vs0"}, "volume": {"name": "fv2"}, "path": "dir2/f5.txt"},
            {"svm": {"name": "vs0"}, "volume": {"name": "fv2"}, "path": "dir2/f6.txt"},
            {
                "svm": {"name": "vs0"},
                "volume": {"name": "fv2"},
                "path": "dir2/f700.txt",
            },
        ],
    }
    resource.reference = {
        "svm": {"name": "vs0"},
        "volume": {"name": "fv1"},
        "path": "dir1/f6.txt",
    }
    resource.post(hydrate=True)
    print(resource)

```

### Moving a file between two FlexGroup volume constituents in the same FlexGroup volume
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileMove

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileMove()
    resource.files_to_move = {
        "sources": [
            {"svm": {"name": "vs0"}, "volume": {"name": "fg2"}, "path": "test/file.txt"}
        ],
        "destinations": [{"volume": {"name": "fg2__0008"}}],
    }
    resource.post(hydrate=True)
    print(resource)

```

### Automatically selecting a destination constituent to move a file in a FlexGroup volume for capacity rebalancing
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileMove

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileMove()
    resource.files_to_move = {
        "sources": [
            {
                "svm": {"name": "vs0"},
                "volume": {"name": "fg2"},
                "path": "test/file2.txt",
            }
        ]
    }
    resource.post(hydrate=True, automatic=True)
    print(resource)

```

### Moving a file between two FlexGroup volume constituents in the same FlexGroup volume using the "force" and "disruptive" options
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileMove

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileMove()
    resource.files_to_move = {
        "sources": [
            {
                "svm": {"name": "vs0"},
                "volume": {"name": "fg2"},
                "path": "test/file3.txt",
            }
        ],
        "destinations": [{"volume": {"name": "fg2__0008"}}],
    }
    resource.post(hydrate=True, force=True, disruptive=True)
    print(resource)

```

### Retrieving file move operations
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileMove

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(FileMove.get_collection()))

```
<div class="try_it_out">
<input id="example6_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example6_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example6_result" class="try_it_out_content">
```
[
    FileMove(
        {
            "index": 0,
            "uuid": "e12bc78d-36bb-4274-8163-fb8c21d59c9b",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "test/file2.txt",
                "volume": {
                    "name": "fg2__0005",
                    "uuid": "4d14f2f6-1c76-11ec-8e1b-005056acf2dd",
                },
            },
            "node": {
                "name": "node1",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/76bc12d1-10aa-11ec-a5b5-005056acf2dd"
                    }
                },
                "uuid": "76bc12d1-10aa-11ec-a5b5-005056acf2dd",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "test/file2.txt",
                "volume": {
                    "name": "fg2__0008",
                    "uuid": "4e919b6d-1c76-11ec-8e1b-005056acf2dd",
                },
            },
        }
    ),
    FileMove(
        {
            "index": 1,
            "uuid": "ce2af347-586d-4b31-b728-1e925f51fdfc",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir2/f2.txt",
                "volume": {
                    "name": "fv2",
                    "uuid": "220bdb3a-26f1-11ec-bf0d-005056acf2dd",
                },
            },
            "node": {
                "name": "node1",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/76bc12d1-10aa-11ec-a5b5-005056acf2dd"
                    }
                },
                "uuid": "76bc12d1-10aa-11ec-a5b5-005056acf2dd",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir1/f2.txt",
                "volume": {
                    "name": "fv1",
                    "uuid": "18fd9110-26f1-11ec-bf0d-005056acf2dd",
                },
            },
        }
    ),
    FileMove(
        {
            "index": 0,
            "uuid": "ce2af347-586d-4b31-b728-1e925f51fdfc",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir2/f1.txt",
                "volume": {
                    "name": "fv2",
                    "uuid": "220bdb3a-26f1-11ec-bf0d-005056acf2dd",
                },
            },
            "node": {
                "name": "node1",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/76bc12d1-10aa-11ec-a5b5-005056acf2dd"
                    }
                },
                "uuid": "76bc12d1-10aa-11ec-a5b5-005056acf2dd",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir1/f1.txt",
                "volume": {
                    "name": "fv1",
                    "uuid": "18fd9110-26f1-11ec-bf0d-005056acf2dd",
                },
            },
        }
    ),
    FileMove(
        {
            "index": 0,
            "uuid": "6d12601b-5377-43bf-99f0-b4bec37565e2",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir2/f3.txt",
                "volume": {
                    "name": "fv2",
                    "uuid": "220bdb3a-26f1-11ec-bf0d-005056acf2dd",
                },
            },
            "node": {
                "name": "node1",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/76bc12d1-10aa-11ec-a5b5-005056acf2dd"
                    }
                },
                "uuid": "76bc12d1-10aa-11ec-a5b5-005056acf2dd",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir1/f3.txt",
                "volume": {
                    "name": "fv1",
                    "uuid": "18fd9110-26f1-11ec-bf0d-005056acf2dd",
                },
            },
        }
    ),
    FileMove(
        {
            "index": 1,
            "uuid": "6d12601b-5377-43bf-99f0-b4bec37565e2",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir2/f4.txt",
                "volume": {
                    "name": "fv2",
                    "uuid": "220bdb3a-26f1-11ec-bf0d-005056acf2dd",
                },
            },
            "node": {
                "name": "node1",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/76bc12d1-10aa-11ec-a5b5-005056acf2dd"
                    }
                },
                "uuid": "76bc12d1-10aa-11ec-a5b5-005056acf2dd",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir1/f4.txt",
                "volume": {
                    "name": "fv1",
                    "uuid": "18fd9110-26f1-11ec-bf0d-005056acf2dd",
                },
            },
        }
    ),
    FileMove(
        {
            "index": 0,
            "uuid": "bbfdface-0d46-4f5f-9624-72f4869eba81",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir2/f5.txt",
                "volume": {
                    "name": "fv2",
                    "uuid": "220bdb3a-26f1-11ec-bf0d-005056acf2dd",
                },
            },
            "node": {
                "name": "node1",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/76bc12d1-10aa-11ec-a5b5-005056acf2dd"
                    }
                },
                "uuid": "76bc12d1-10aa-11ec-a5b5-005056acf2dd",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir1/f5.txt",
                "volume": {
                    "name": "fv1",
                    "uuid": "18fd9110-26f1-11ec-bf0d-005056acf2dd",
                },
            },
        }
    ),
    FileMove(
        {
            "index": 2,
            "uuid": "bbfdface-0d46-4f5f-9624-72f4869eba81",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir2/f700.txt",
                "volume": {
                    "name": "fv2",
                    "uuid": "220bdb3a-26f1-11ec-bf0d-005056acf2dd",
                },
            },
            "node": {
                "name": "node1",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/76bc12d1-10aa-11ec-a5b5-005056acf2dd"
                    }
                },
                "uuid": "76bc12d1-10aa-11ec-a5b5-005056acf2dd",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir1/f7.txt",
                "volume": {
                    "name": "fv1",
                    "uuid": "18fd9110-26f1-11ec-bf0d-005056acf2dd",
                },
            },
        }
    ),
    FileMove(
        {
            "index": 1,
            "uuid": "bbfdface-0d46-4f5f-9624-72f4869eba81",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir2/f6.txt",
                "volume": {
                    "name": "fv2",
                    "uuid": "220bdb3a-26f1-11ec-bf0d-005056acf2dd",
                },
            },
            "node": {
                "name": "node1",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/76bc12d1-10aa-11ec-a5b5-005056acf2dd"
                    }
                },
                "uuid": "76bc12d1-10aa-11ec-a5b5-005056acf2dd",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir1/f6.txt",
                "volume": {
                    "name": "fv1",
                    "uuid": "18fd9110-26f1-11ec-bf0d-005056acf2dd",
                },
            },
        }
    ),
    FileMove(
        {
            "index": 0,
            "uuid": "6591a42a-4ea2-4d40-bfb4-38959f6bd68e",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "/test/file.txt",
                "volume": {
                    "name": "fg2__0008",
                    "uuid": "4e919b6d-1c76-11ec-8e1b-005056acf2dd",
                },
            },
            "node": {
                "name": "node2",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/780255d2-10aa-11ec-a308-005056acf86d"
                    }
                },
                "uuid": "780255d2-10aa-11ec-a308-005056acf86d",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "test/file.txt",
                "volume": {
                    "name": "fg2__0001",
                    "uuid": "438731dd-1c76-11ec-8e1b-005056acf2dd",
                },
            },
        }
    ),
    FileMove(
        {
            "index": 0,
            "uuid": "1a94e95a-346e-4eb3-969a-110e275cbf18",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "/test/file3.txt",
                "volume": {
                    "name": "fg2__0008",
                    "uuid": "4e919b6d-1c76-11ec-8e1b-005056acf2dd",
                },
            },
            "node": {
                "name": "node2",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/780255d2-10aa-11ec-a308-005056acf86d"
                    }
                },
                "uuid": "780255d2-10aa-11ec-a308-005056acf86d",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "test/file3.txt",
                "volume": {
                    "name": "fg2__0001",
                    "uuid": "438731dd-1c76-11ec-8e1b-005056acf2dd",
                },
            },
        }
    ),
]

```
</div>
</div>

### Retrieving all moves in a file move operation
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileMove

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileMove(
        index="*", uuid="ce2af347-586d-4b31-b728-1e925f51fdfc", **{"node.uuid": "*"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example7_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example7_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example7_result" class="try_it_out_content">
```
[
    FileMove(
        {
            "index": 1,
            "uuid": "ce2af347-586d-4b31-b728-1e925f51fdfc",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir2/f2.txt",
                "volume": {
                    "name": "fv2",
                    "uuid": "220bdb3a-26f1-11ec-bf0d-005056acf2dd",
                },
            },
            "node": {
                "name": "node1",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/76bc12d1-10aa-11ec-a5b5-005056acf2dd"
                    }
                },
                "uuid": "76bc12d1-10aa-11ec-a5b5-005056acf2dd",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir1/f2.txt",
                "volume": {
                    "name": "fv1",
                    "uuid": "18fd9110-26f1-11ec-bf0d-005056acf2dd",
                },
            },
        }
    ),
    FileMove(
        {
            "index": 0,
            "uuid": "ce2af347-586d-4b31-b728-1e925f51fdfc",
            "destination": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir2/f1.txt",
                "volume": {
                    "name": "fv2",
                    "uuid": "220bdb3a-26f1-11ec-bf0d-005056acf2dd",
                },
            },
            "node": {
                "name": "node1",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/76bc12d1-10aa-11ec-a5b5-005056acf2dd"
                    }
                },
                "uuid": "76bc12d1-10aa-11ec-a5b5-005056acf2dd",
            },
            "source": {
                "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
                "path": "dir1/f1.txt",
                "volume": {
                    "name": "fv1",
                    "uuid": "18fd9110-26f1-11ec-bf0d-005056acf2dd",
                },
            },
        }
    ),
]

```
</div>
</div>

### Retrieving a specific file move in a file move operation
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileMove

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileMove(
        index=1, uuid="ce2af347-586d-4b31-b728-1e925f51fdfc", **{"node.uuid": "*"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example8_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example8_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example8_result" class="try_it_out_content">
```
FileMove(
    {
        "index": 1,
        "uuid": "ce2af347-586d-4b31-b728-1e925f51fdfc",
        "destination": {
            "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
            "path": "dir2/f2.txt",
            "volume": {"name": "fv2", "uuid": "220bdb3a-26f1-11ec-bf0d-005056acf2dd"},
        },
        "node": {
            "name": "node1",
            "_links": {
                "self": {
                    "href": "/api/cluster/nodes/76bc12d1-10aa-11ec-a5b5-005056acf2dd"
                }
            },
            "uuid": "76bc12d1-10aa-11ec-a5b5-005056acf2dd",
        },
        "source": {
            "svm": {"name": "vs0", "uuid": "5b2c8638-10bc-11ec-8e1b-005056acf2dd"},
            "path": "dir1/f2.txt",
            "volume": {"name": "fv1", "uuid": "18fd9110-26f1-11ec-bf0d-005056acf2dd"},
        },
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


__all__ = ["FileMove", "FileMoveSchema"]
__pdoc__ = {
    "FileMoveSchema.resource": False,
    "FileMoveSchema.opts": False,
    "FileMove.file_move_show": False,
    "FileMove.file_move_create": False,
    "FileMove.file_move_modify": False,
    "FileMove.file_move_delete": False,
}


class FileMoveSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FileMove object"""

    cutover_time = Size(
        data_key="cutover_time",
    )
    r""" Time that the file move operation takes before cutover completes, in seconds.

Example: 8"""

    destination = fields.Nested("netapp_ontap.models.file_move_file.FileMoveFileSchema", data_key="destination", unknown=EXCLUDE)
    r""" The destination field of the file_move."""

    elapsed_time = Size(
        data_key="elapsed_time",
    )
    r""" Time elapsed since the start of the file move operation, in seconds.

Example: 100"""

    failure = fields.Nested("netapp_ontap.models.error.ErrorSchema", data_key="failure", unknown=EXCLUDE)
    r""" Contains the most recent failure reason for move operation."""

    files_to_move = fields.Nested("netapp_ontap.models.file_move_files_to_move.FileMoveFilesToMoveSchema", data_key="files_to_move", unknown=EXCLUDE)
    r""" The files_to_move field of the file_move."""

    index = Size(
        data_key="index",
    )
    r""" An additional unique element identifying one file among many that could possibly be moved as part of a job. File index is an identifier ordered by the file path arrays provided during create. For file move operations that involve only one file, the file-index value of zero is always correct.


Example: 0"""

    is_destination_ready = fields.Boolean(
        data_key="is_destination_ready",
    )
    r""" Indicates whether the destination file is ready for use."""

    is_flexgroup = fields.Boolean(
        data_key="is_flexgroup",
    )
    r""" Indicates whether this is a FlexGroup file move operation."""

    is_snapshot_fenced = fields.Boolean(
        data_key="is_snapshot_fenced",
    )
    r""" Indicates whether Snapshot copies are fenced."""

    max_cutover_time = Size(
        data_key="max_cutover_time",
    )
    r""" The maximum amount of time, in seconds that the source can be quiesced before a destination file must be made available for read-write traffic. Not supported in FlexGroup volume file move operations.


Example: 10"""

    max_throughput = Size(
        data_key="max_throughput",
    )
    r""" Maximum amount of data, in bytes that can be transferred per second in support of this operation. A non-zero value less than 1 MB/s is set to 1 MB/s. A non-zero value greater than 1 MB/s is truncated to the nearest integral megabyte value. If unspecified, the default value is "0" which means no range is set for the data transfer.

Example: 250000"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", data_key="node", unknown=EXCLUDE)
    r""" The node field of the file_move."""

    reference = fields.Nested("netapp_ontap.models.reference_file_move_file.ReferenceFileMoveFileSchema", data_key="reference", unknown=EXCLUDE)
    r""" The reference field of the file_move."""

    scanner = fields.Nested("netapp_ontap.models.file_move_scanner.FileMoveScannerSchema", data_key="scanner", unknown=EXCLUDE)
    r""" The scanner field of the file_move."""

    source = fields.Nested("netapp_ontap.models.file_move_file.FileMoveFileSchema", data_key="source", unknown=EXCLUDE)
    r""" The source field of the file_move."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the file_move."""

    uuid = fields.Str(
        data_key="uuid",
    )
    r""" The UUID which uniquely identifies the job that started this move operation.

Example: 4fcb3159-a4ee-42b5-bb16-f752f2c430fc"""

    volume = fields.Nested("netapp_ontap.resources.volume.VolumeSchema", data_key="volume", unknown=EXCLUDE)
    r""" The volume field of the file_move."""

    @property
    def resource(self):
        return FileMove

    gettable_fields = [
        "cutover_time",
        "destination",
        "elapsed_time",
        "failure",
        "index",
        "is_destination_ready",
        "is_flexgroup",
        "is_snapshot_fenced",
        "max_cutover_time",
        "max_throughput",
        "node.links",
        "node.name",
        "node.uuid",
        "scanner",
        "source",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "uuid",
        "volume.links",
        "volume.name",
        "volume.uuid",
    ]
    """cutover_time,destination,elapsed_time,failure,index,is_destination_ready,is_flexgroup,is_snapshot_fenced,max_cutover_time,max_throughput,node.links,node.name,node.uuid,scanner,source,svm.links,svm.name,svm.uuid,uuid,volume.links,volume.name,volume.uuid,"""

    patchable_fields = [
        "destination",
        "node.name",
        "node.uuid",
        "scanner",
        "source",
    ]
    """destination,node.name,node.uuid,scanner,source,"""

    postable_fields = [
        "destination",
        "files_to_move",
        "max_cutover_time",
        "max_throughput",
        "node.name",
        "node.uuid",
        "reference",
        "scanner",
        "source",
    ]
    """destination,files_to_move,max_cutover_time,max_throughput,node.name,node.uuid,reference,scanner,source,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in FileMove.get_collection(fields=field)]
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
            raise NetAppRestError("FileMove modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class FileMove(Resource):
    r""" Details of a file move operation. """

    _schema = FileMoveSchema
    _path = "/api/storage/file/moves"
    _keys = ["node.uuid", "uuid", "index"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves all ongoing file move operations in the cluster.
### Related ONTAP commands
* `volume file move show`
* `volume rebalance file-move show`

### Learn more
* [`DOC /storage/file/moves`](#docs-storage-storage_file_moves)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="file move show")
        def file_move_show(
            fields: List[Choices.define(["cutover_time", "elapsed_time", "index", "is_destination_ready", "is_flexgroup", "is_snapshot_fenced", "max_cutover_time", "max_throughput", "uuid", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of FileMove resources

            Args:
                cutover_time: Time that the file move operation takes before cutover completes, in seconds.
                elapsed_time: Time elapsed since the start of the file move operation, in seconds.
                index: An additional unique element identifying one file among many that could possibly be moved as part of a job. File index is an identifier ordered by the file path arrays provided during create. For file move operations that involve only one file, the file-index value of zero is always correct. 
                is_destination_ready: Indicates whether the destination file is ready for use.
                is_flexgroup: Indicates whether this is a FlexGroup file move operation.
                is_snapshot_fenced: Indicates whether Snapshot copies are fenced.
                max_cutover_time: The maximum amount of time, in seconds that the source can be quiesced before a destination file must be made available for read-write traffic. Not supported in FlexGroup volume file move operations. 
                max_throughput: Maximum amount of data, in bytes that can be transferred per second in support of this operation. A non-zero value less than 1 MB/s is set to 1 MB/s. A non-zero value greater than 1 MB/s is truncated to the nearest integral megabyte value. If unspecified, the default value is \"0\" which means no range is set for the data transfer.
                uuid: The UUID which uniquely identifies the job that started this move operation.
            """

            kwargs = {}
            if cutover_time is not None:
                kwargs["cutover_time"] = cutover_time
            if elapsed_time is not None:
                kwargs["elapsed_time"] = elapsed_time
            if index is not None:
                kwargs["index"] = index
            if is_destination_ready is not None:
                kwargs["is_destination_ready"] = is_destination_ready
            if is_flexgroup is not None:
                kwargs["is_flexgroup"] = is_flexgroup
            if is_snapshot_fenced is not None:
                kwargs["is_snapshot_fenced"] = is_snapshot_fenced
            if max_cutover_time is not None:
                kwargs["max_cutover_time"] = max_cutover_time
            if max_throughput is not None:
                kwargs["max_throughput"] = max_throughput
            if uuid is not None:
                kwargs["uuid"] = uuid
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return FileMove.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all FileMove resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)


    @classmethod
    def post_collection(
        cls,
        records: Iterable["FileMove"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["FileMove"], NetAppResponse]:
        r"""Starts a file move operation. This API can be used to move files from one FlexVol volume to another FlexVol volume or within a FlexGroup volume for capacity rebalancing.
For a FlexGroup volume file move operation, only one source file can be specified in files_to_move. The source volume is the FlexGroup volume. The destination volume is the destination FlexGroup volume constituent to move the file to. When ``automatic`` is true, destination volume is not required. The source path is the path to the file to be moved within the FlexGroup volume. If the destination path is specified, it must be the same as the source path.
## Required properties for file move operation
* `files_to_move` - List of files with the destination they are to be moved to.
## Optional properties for file move operation
* `reference` - The source reference file for moving multiple files.
## Default property values
* `max_throughput` - _0_
* `max_cutover_time` - _10_
* `reference.max_cutover_time` - _10_
## Related ONTAP commands
* `volume file move start`
* `volume rebalance file-move start`

### Learn more
* [`DOC /storage/file/moves`](#docs-storage-storage_file_moves)"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)


    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves all ongoing file move operations in the cluster.
### Related ONTAP commands
* `volume file move show`
* `volume rebalance file-move show`

### Learn more
* [`DOC /storage/file/moves`](#docs-storage-storage_file_moves)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""## Overview
Retrieve the status of an on-going file move operation.
### Related ONTAP commands
* `volume file move show`
* `volume rebalance file-move show`

### Learn more
* [`DOC /storage/file/moves`](#docs-storage-storage_file_moves)"""
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
        r"""Starts a file move operation. This API can be used to move files from one FlexVol volume to another FlexVol volume or within a FlexGroup volume for capacity rebalancing.
For a FlexGroup volume file move operation, only one source file can be specified in files_to_move. The source volume is the FlexGroup volume. The destination volume is the destination FlexGroup volume constituent to move the file to. When ``automatic`` is true, destination volume is not required. The source path is the path to the file to be moved within the FlexGroup volume. If the destination path is specified, it must be the same as the source path.
## Required properties for file move operation
* `files_to_move` - List of files with the destination they are to be moved to.
## Optional properties for file move operation
* `reference` - The source reference file for moving multiple files.
## Default property values
* `max_throughput` - _0_
* `max_cutover_time` - _10_
* `reference.max_cutover_time` - _10_
## Related ONTAP commands
* `volume file move start`
* `volume rebalance file-move start`

### Learn more
* [`DOC /storage/file/moves`](#docs-storage-storage_file_moves)"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="file move create")
        async def file_move_create(
        ) -> ResourceTable:
            """Create an instance of a FileMove resource

            Args:
                cutover_time: Time that the file move operation takes before cutover completes, in seconds.
                destination: 
                elapsed_time: Time elapsed since the start of the file move operation, in seconds.
                failure: Contains the most recent failure reason for move operation.
                files_to_move: 
                index: An additional unique element identifying one file among many that could possibly be moved as part of a job. File index is an identifier ordered by the file path arrays provided during create. For file move operations that involve only one file, the file-index value of zero is always correct. 
                is_destination_ready: Indicates whether the destination file is ready for use.
                is_flexgroup: Indicates whether this is a FlexGroup file move operation.
                is_snapshot_fenced: Indicates whether Snapshot copies are fenced.
                max_cutover_time: The maximum amount of time, in seconds that the source can be quiesced before a destination file must be made available for read-write traffic. Not supported in FlexGroup volume file move operations. 
                max_throughput: Maximum amount of data, in bytes that can be transferred per second in support of this operation. A non-zero value less than 1 MB/s is set to 1 MB/s. A non-zero value greater than 1 MB/s is truncated to the nearest integral megabyte value. If unspecified, the default value is \"0\" which means no range is set for the data transfer.
                node: 
                reference: 
                scanner: 
                source: 
                svm: 
                uuid: The UUID which uniquely identifies the job that started this move operation.
                volume: 
            """

            kwargs = {}
            if cutover_time is not None:
                kwargs["cutover_time"] = cutover_time
            if destination is not None:
                kwargs["destination"] = destination
            if elapsed_time is not None:
                kwargs["elapsed_time"] = elapsed_time
            if failure is not None:
                kwargs["failure"] = failure
            if files_to_move is not None:
                kwargs["files_to_move"] = files_to_move
            if index is not None:
                kwargs["index"] = index
            if is_destination_ready is not None:
                kwargs["is_destination_ready"] = is_destination_ready
            if is_flexgroup is not None:
                kwargs["is_flexgroup"] = is_flexgroup
            if is_snapshot_fenced is not None:
                kwargs["is_snapshot_fenced"] = is_snapshot_fenced
            if max_cutover_time is not None:
                kwargs["max_cutover_time"] = max_cutover_time
            if max_throughput is not None:
                kwargs["max_throughput"] = max_throughput
            if node is not None:
                kwargs["node"] = node
            if reference is not None:
                kwargs["reference"] = reference
            if scanner is not None:
                kwargs["scanner"] = scanner
            if source is not None:
                kwargs["source"] = source
            if svm is not None:
                kwargs["svm"] = svm
            if uuid is not None:
                kwargs["uuid"] = uuid
            if volume is not None:
                kwargs["volume"] = volume

            resource = FileMove(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create FileMove: %s" % err)
            return [resource]




