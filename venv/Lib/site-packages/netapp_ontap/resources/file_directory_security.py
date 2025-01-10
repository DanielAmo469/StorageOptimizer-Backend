r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
Using this API, You can manage NTFS file security and audit policies of file or directory without the need of a client. It works similar to what you could do with a cacls in windows client. It will create an NTFS security descriptor(SD) to which you can add access control entries (ACEs) to the discretionary access control list (DACL) and the system access control list (SACL). Generally, an SD contains following information:

 * Security identifiers (SIDs) for the owner and primary group of an object. A security identifier (SID) is a unique value of variable length used to identify a trustee. Each account has a unique SID issued by an authority, such as a Windows domain controller, and is stored in a security database.
 * A DACL  identifies the trustees that are allowed or denied access to a securable object. When a process tries to access a securable object, the system checks the ACEs in the object's DACL to determine whether to grant access to it.
 * A SACL  enables administrators to log attempts to access a secured object. Each ACE specifies the types of access attempts by a specified trustee that cause the system to generate a record in the security event log. An ACE in a SACL can generate audit records when an access attempt fails, when it succeeds, or both.
 * A set of control bits that qualify the meaning of a SD or its individual members.
####
Currently, in ONTAP CLI, creating and applying NTFS ACLs is a 5-step process:

 * Create an SD.
 * Add DACLs and SACLs to the NTFS SD. If you want to audit file and directory events, you must configure auditing on the Vserver, in addition, to adding a SACL to the SD.
 * Create a file/directory security policy. This step associates the policy with a SVM.
 * Create a policy task. A policy task refers to a single operation to apply to a file (or folder) or to a set of files (or folders). Among other things, the task defines which SD to apply to a path.
 * Apply a policy to the associated SVM.
####
This REST API to set the DACL/SACL is similar to the windows GUI. The approach used here has been simplified by combining all steps into a single step. The REST API uses only minimal and mandatory parameters to create access control entries (ACEs), which can be added to the discretionary access control list (DACL) and the system access control list (SACL). Based on information provided, SD is created and  applied on the target path.</br>
Beginning with ONTAP 9.10.1, SLAG (Storage-Level Access Guard) ACLs can also be configured through these endpoints. SLAG is designed to be set on a volume or qtree. Storage-level security cannot be revoked from a client, not even by a system (Windows or UNIX) administrator. It is designed to be modified by storage administrators only, which precedes the share/export permission and the Windows ACLs or UNIX mode bits. Similar to configuring file-directory ACLs, configuring SLAG ACLs is also simplified by combining all steps into a single step.
## Examples
### Creating a new SD
Use this endpoint to apply a fresh set of SACLs and DACLs. A new SD is created based on the input parameters and it replaces the old SD for the given target path:
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileDirectorySecurity

with HostConnection(
    "10.140.101.39", username="admin", password="password", verify=False
):
    resource = FileDirectorySecurity("9479099d-5b9f-11eb-9c4e-0050568e8682", "/parent")
    resource.acls = [
        {
            "access": "access_allow",
            "advanced_rights": {
                "append_data": True,
                "delete": True,
                "delete_child": True,
                "execute_file": True,
                "full_control": True,
                "read_attr": True,
                "read_data": True,
                "read_ea": True,
                "read_perm": True,
                "write_attr": True,
                "write_data": True,
                "write_ea": True,
                "write_owner": True,
                "write_perm": True,
            },
            "apply_to": {"files": True, "sub_folders": True, "this_folder": True},
            "user": "administrator",
        }
    ]
    resource.control_flags = "32788"
    resource.group = "S-1-5-21-2233347455-2266964949-1780268902-69700"
    resource.ignore_paths = ["/parent/child2"]
    resource.owner = "S-1-5-21-2233347455-2266964949-1780268902-69304"
    resource.propagation_mode = "propagate"
    resource.post(hydrate=True, return_timeout=0)
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
FileDirectorySecurity(
    {
        "propagation_mode": "propagate",
        "group": "S-1-5-21-2233347455-2266964949-1780268902-69700",
        "acls": [
            {
                "access": "access_allow",
                "apply_to": {"this_folder": True, "files": True, "sub_folders": True},
                "user": "administrator",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            }
        ],
        "ignore_paths": ["/parent/child2"],
        "owner": "S-1-5-21-2233347455-2266964949-1780268902-69304",
        "control_flags": "32788",
    }
)

```
</div>
</div>

---
### Configuring a new set of SLAG DACLs and SACLs
Use this endpoint to apply a fresh set of SLAG DACLs and SACLs. A new SD is created based on the input parameters and it replaces the old SLAG pemissions for the given target path:
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileDirectorySecurity

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileDirectorySecurity(
        "9f738ac5-c502-11eb-b82c-0050568e5902", "/test_vol"
    )
    resource.access_control = "slag"
    resource.acls = [
        {
            "access": "access_allow",
            "advanced_rights": {
                "append_data": True,
                "delete": True,
                "delete_child": True,
                "execute_file": True,
                "full_control": True,
                "read_attr": True,
                "read_data": True,
                "read_ea": True,
                "read_perm": True,
                "write_attr": True,
                "write_data": True,
                "write_ea": True,
                "write_owner": True,
                "write_perm": True,
            },
            "apply_to": {"files": True, "sub_folders": True, "this_folder": True},
            "user": "user1",
        },
        {
            "access": "audit_success",
            "advanced_rights": {
                "append_data": True,
                "delete": True,
                "delete_child": True,
                "execute_file": True,
                "full_control": True,
                "read_attr": True,
                "read_data": True,
                "read_ea": True,
                "read_perm": True,
                "write_attr": True,
                "write_data": True,
                "write_ea": True,
                "write_owner": True,
                "write_perm": True,
            },
            "apply_to": {"files": True, "sub_folders": True, "this_folder": True},
            "user": "user2",
        },
    ]
    resource.post(hydrate=True, return_timeout=0)
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
FileDirectorySecurity(
    {
        "acls": [
            {
                "access": "access_allow",
                "apply_to": {"this_folder": True, "files": True, "sub_folders": True},
                "user": "user1",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            },
            {
                "access": "audit_success",
                "apply_to": {"this_folder": True, "files": True, "sub_folders": True},
                "user": "user2",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            },
        ],
        "access_control": "slag",
    }
)

```
</div>
</div>

---
### Retrieving file permissions
Use this endpoint to retrieve all the security and auditing information of a directory or file:
</br>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileDirectorySecurity

with HostConnection(
    "10.140.101.39", username="admin", password="password", verify=False
):
    resource = FileDirectorySecurity("9479099d-5b9f-11eb-9c4e-0050568e8682", "/parent")
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example2_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example2_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example2_result" class="try_it_out_content">
```
FileDirectorySecurity(
    {
        "inode": 64,
        "effective_style": "ntfs",
        "user_id": "0",
        "group": "BUILTIN\\Administrators",
        "group_id": "0",
        "text_dos_attr": "----D---",
        "mode_bits": 777,
        "text_mode_bits": "rwxrwxrwx",
        "acls": [
            {
                "access": "access_allow",
                "apply_to": {"this_folder": True, "files": True, "sub_folders": True},
                "user": "BUILTIN\\Administrators",
                "access_control": "file_directory",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "synchronize": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            },
            {
                "access": "access_allow",
                "apply_to": {"this_folder": True, "files": True, "sub_folders": True},
                "user": "BUILTIN\\Users",
                "access_control": "file_directory",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "synchronize": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            },
            {
                "access": "access_allow",
                "apply_to": {"this_folder": True, "files": True, "sub_folders": True},
                "user": "CREATOR OWNER",
                "access_control": "file_directory",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "synchronize": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            },
            {
                "access": "access_allow",
                "apply_to": {"this_folder": True, "files": True, "sub_folders": True},
                "user": "Everyone",
                "access_control": "file_directory",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "synchronize": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            },
            {
                "access": "access_allow",
                "apply_to": {"this_folder": True, "files": True, "sub_folders": True},
                "user": "NT AUTHORITY\\SYSTEM",
                "access_control": "file_directory",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "synchronize": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            },
            {
                "access": "access_allow",
                "apply_to": {"this_folder": True, "sub_folders": True},
                "user": "user1",
                "access_control": "slag",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "synchronize": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            },
            {
                "access": "access_allow",
                "apply_to": {"files": True},
                "user": "user1",
                "access_control": "slag",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "synchronize": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            },
            {
                "access": "audit_success",
                "apply_to": {"this_folder": True, "sub_folders": True},
                "user": "user2",
                "access_control": "slag",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "synchronize": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            },
            {
                "access": "audit_success",
                "apply_to": {"files": True},
                "user": "user2",
                "access_control": "slag",
                "advanced_rights": {
                    "execute_file": True,
                    "read_perm": True,
                    "write_attr": True,
                    "read_ea": True,
                    "full_control": True,
                    "write_perm": True,
                    "delete_child": True,
                    "delete": True,
                    "synchronize": True,
                    "read_attr": True,
                    "write_data": True,
                    "append_data": True,
                    "write_ea": True,
                    "write_owner": True,
                    "read_data": True,
                },
            },
        ],
        "owner": "BUILTIN\\Administrators",
        "control_flags": "0x8014",
        "security_style": "mixed",
        "dos_attributes": "10",
    }
)

```
</div>
</div>

---
### Updating SD-specific information
Use this end point to update the following information:

 * Primary owner of the file/directory.
 * Primary group of the file/directory.
 * Control flags associated with with SD of the file/directory.
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileDirectorySecurity

with HostConnection(
    "10.140.101.39", username="admin", password="password", verify=False
):
    resource = FileDirectorySecurity("9479099d-5b9f-11eb-9c4e-0050568e8682", "/parent")
    resource.control_flags = "32788"
    resource.group = "everyone"
    resource.owner = "user1"
    resource.patch(hydrate=True, return_timeout=0)

```

---
### Removing all SLAG ACLs
Use this end point to remove all SLAG ACLs.
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileDirectorySecurity

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileDirectorySecurity(
        "713f569f-d4bc-11eb-b24a-005056ac6ce1", "/test_vol"
    )
    resource.delete(access_control="slag")

```

---
### Adding a single file-directory DACL/SACL ACE
Use this endpoint to add a single SACL/DACL ACE for a new user or for an existing user with a different access type (allow or deny). The given ACE is merged with an existing SACL/DACL and based on the type of “propagation-mode”, it is reflected to the child object:
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileDirectorySecurityAcl

with HostConnection(
    "10.140.101.39", username="admin", password="password", verify=False
):
    resource = FileDirectorySecurityAcl(
        "9479099d-5b9f-11eb-9c4e-0050568e8682", "/parent"
    )
    resource.access = "access_allow"
    resource.apply_to = {"files": True, "sub_folders": True, "this_folder": True}
    resource.ignore_paths = ["/parent/child2"]
    resource.propagation_mode = "propagate"
    resource.rights = "read"
    resource.user = "himanshu"
    resource.post(hydrate=True, return_timeout=0, return_records=False)
    print(resource)

```
<div class="try_it_out">
<input id="example5_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example5_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example5_result" class="try_it_out_content">
```
FileDirectorySecurityAcl(
    {
        "propagation_mode": "propagate",
        "access": "access_allow",
        "apply_to": {"this_folder": True, "files": True, "sub_folders": True},
        "user": "himanshu",
        "ignore_paths": ["/parent/child2"],
        "rights": "read",
    }
)

```
</div>
</div>

---
### Adding a single SLAG DACL/SACL ACE
Use this endpoint to add a single SLAG SACL/DACL ACE to an existing set of ACLs for a user or for an existing user with a different access type (allow or deny).
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileDirectorySecurityAcl

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileDirectorySecurityAcl(
        "713f569f-d4bc-11eb-b24a-005056ac6ce1", "/test_vol"
    )
    resource.access = "access_allow"
    resource.access_control = "slag"
    resource.advanced_rights = {
        "append_data": True,
        "delete": True,
        "delete_child": True,
        "execute_file": True,
        "full_control": True,
        "read_attr": True,
        "read_data": True,
        "read_ea": True,
        "read_perm": True,
        "write_attr": True,
        "write_data": True,
        "write_ea": True,
        "write_owner": True,
        "write_perm": True,
    }
    resource.apply_to = {"files": True, "sub_folders": True, "this_folder": True}
    resource.user = "user1"
    resource.post(hydrate=True, return_timeout=0, return_records=False)
    print(resource)

```
<div class="try_it_out">
<input id="example6_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example6_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example6_result" class="try_it_out_content">
```
FileDirectorySecurityAcl(
    {
        "access": "access_allow",
        "apply_to": {"this_folder": True, "files": True, "sub_folders": True},
        "user": "user1",
        "access_control": "slag",
        "advanced_rights": {
            "execute_file": True,
            "read_perm": True,
            "write_attr": True,
            "read_ea": True,
            "full_control": True,
            "write_perm": True,
            "delete_child": True,
            "delete": True,
            "read_attr": True,
            "write_data": True,
            "append_data": True,
            "write_ea": True,
            "write_owner": True,
            "read_data": True,
        },
    }
)

```
</div>
</div>

---
### Updating existing SACL/DACL ACE
Use this endpoint to update the rights/advanced rights for an existing user, for a specified path. You cannot update the access type using this end point. Based on the type of  “propagation-mode”, it is reflected to the child object:
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileDirectorySecurityAcl

with HostConnection(
    "10.140.101.39", username="admin", password="password", verify=False
):
    resource = FileDirectorySecurityAcl(
        "9479099d-5b9f-11eb-9c4e-0050568e8682", "/parent", user="himanshu"
    )
    resource.access = "access_allow"
    resource.advanced_rights = {
        "append_data": True,
        "delete": True,
        "delete_child": True,
        "execute_file": True,
        "full_control": True,
        "read_attr": False,
        "read_data": False,
        "read_ea": False,
        "read_perm": False,
        "write_attr": True,
        "write_data": True,
        "write_ea": True,
        "write_owner": True,
        "write_perm": True,
    }
    resource.apply_to = {"files": True, "sub_folders": True, "this_folder": True}
    resource.ignore_paths = ["/parent/child2"]
    resource.propagation_mode = "propagate"
    resource.patch(hydrate=True, return_timeout=0)

```

---
### Updating an existing SLG SACL/DACL ACE
Use this endpoint to update the SLAG rights/advanced rights for an existing user, for a specified path. You cannot update the access type using this end point.
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileDirectorySecurityAcl

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileDirectorySecurityAcl(
        "713f569f-d4bc-11eb-b24a-005056ac6ce1", "/test_vol", user="user1"
    )
    resource.access = "access_allow"
    resource.access_control = "slag"
    resource.apply_to = {"files": True, "sub_folders": True, "this_folder": True}
    resource.rights = "read"
    resource.patch(hydrate=True, return_records=False, return_timeout=0)

```

---
### Deleting an existing SACL/DACL ACE
Use this endpoint to delete any of the existing rights/advanced_rights for a user. Based on the type of “propagation-mode”, it is reflected to the child object:
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileDirectorySecurityAcl

with HostConnection(
    "10.140.101.39", username="admin", password="password", verify=False
):
    resource = FileDirectorySecurityAcl(
        "9479099d-5b9f-11eb-9c4e-0050568e8682", "/parent", user="himanshu"
    )
    resource.delete(
        body={
            "access": "access_allow",
            "apply_to": {"files": True, "sub_folders": True, "this_folder": True},
            "ignore_paths": ["/parent/child2"],
            "propagation_mode": "propagate",
        },
        return_timeout=0,
    )

```

---
### Deleting an existing SLAG SACL/DACL ACE
Use this endpoint to delete any SLAG ACE for a user.
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import FileDirectorySecurityAcl

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = FileDirectorySecurityAcl(
        "713f569f-d4bc-11eb-b24a-005056ac6ce1", "/test_vol", user="user1"
    )
    resource.delete(
        body={
            "access": "access_allow",
            "access_control": "slag",
            "apply_to": {"files": True, "sub_folders": True, "this_folder": True},
        },
        return_records=False,
        return_timeout=0,
    )

```

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


__all__ = ["FileDirectorySecurity", "FileDirectorySecuritySchema"]
__pdoc__ = {
    "FileDirectorySecuritySchema.resource": False,
    "FileDirectorySecuritySchema.opts": False,
    "FileDirectorySecurity.file_directory_security_show": False,
    "FileDirectorySecurity.file_directory_security_create": False,
    "FileDirectorySecurity.file_directory_security_modify": False,
    "FileDirectorySecurity.file_directory_security_delete": False,
}


class FileDirectorySecuritySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FileDirectorySecurity object"""

    access_control = fields.Str(
        data_key="access_control",
        validate=enum_validation(['file_directory', 'slag']),
    )
    r""" An Access Control Level specifies the access control of the task to be applied. Valid values
are "file-directory" or "Storage-Level Access Guard (SLAG)". SLAG is used to apply the
specified security descriptors with the task for the volume or qtree. Otherwise, the
security descriptors are applied on files and directories at the specified path.
The value slag is not supported on FlexGroups volumes. The default value is "file-directory".


Valid choices:

* file_directory
* slag"""

    acls = fields.List(fields.Nested("netapp_ontap.models.acl.AclSchema", unknown=EXCLUDE), data_key="acls")
    r""" A discretionary access security list (DACL) identifies the trustees that are allowed or denied access
to a securable object. When a process tries to access a securable
object, the system checks the access control entries (ACEs) in the
object's DACL to determine whether to grant access to it."""

    control_flags = fields.Str(
        data_key="control_flags",
    )
    r""" Specifies the control flags in the SD. It is a Hexadecimal Value.


Example: 8014"""

    dos_attributes = fields.Str(
        data_key="dos_attributes",
    )
    r""" Specifies the file attributes on this file or directory.


Example: 10"""

    effective_style = fields.Str(
        data_key="effective_style",
        validate=enum_validation(['unix', 'ntfs', 'mixed', 'unified']),
    )
    r""" Specifies the effective style of the SD. The following values are supported:

* unix - UNIX style
* ntfs - NTFS style
* mixed - Mixed style
* unified - Unified style


Valid choices:

* unix
* ntfs
* mixed
* unified"""

    group = fields.Str(
        data_key="group",
    )
    r""" Specifies the owner's primary group.
You can specify the owner group using either a group name or SID.


Example: S-1-5-21-2233347455-2266964949-1780268902-69700"""

    group_id = fields.Str(
        data_key="group_id",
    )
    r""" Specifies group ID on this file or directory.


Example: 2"""

    ignore_paths = fields.List(fields.Str, data_key="ignore_paths")
    r""" Specifies that permissions on this file or directory cannot be replaced.


Example: ["/dir1/dir2/","/parent/dir3"]"""

    inode = Size(
        data_key="inode",
    )
    r""" Specifies the File Inode number.


Example: 64"""

    mode_bits = Size(
        data_key="mode_bits",
    )
    r""" Specifies the mode bits on this file or directory.


Example: 777"""

    owner = fields.Str(
        data_key="owner",
    )
    r""" Specifies the owner of the SD.
You can specify the owner using either a user name or security identifier (SID).
The owner of the SD can modify the permissions on the
file (or folder) or files (or folders) to which the SD
is applied and can give other users the right to take ownership
of the object or objects to which the SD is applied.


Example: S-1-5-21-2233347455-2266964949-1780268902-69304"""

    propagation_mode = fields.Str(
        data_key="propagation_mode",
        validate=enum_validation(['propagate', 'replace']),
    )
    r""" Specifies how to propagate security settings to child subfolders and files.
This setting determines how child files/folders contained within a parent
folder inherit access control and audit information from the parent folder.
The available values are:

* propagate    - propagate inheritable permissions to all subfolders and files
* replace      - replace existing permissions on all subfolders and files with inheritable permissions


Valid choices:

* propagate
* replace"""

    security_style = fields.Str(
        data_key="security_style",
        validate=enum_validation(['unix', 'ntfs', 'mixed', 'unified']),
    )
    r""" Specifies the security style of the SD. The following values are supported:

* unix - UNIX style
* ntfs - NTFS style
* mixed - Mixed style
* unified - Unified style


Valid choices:

* unix
* ntfs
* mixed
* unified"""

    text_dos_attr = fields.Str(
        data_key="text_dos_attr",
    )
    r""" Specifies the textual format of file attributes on this file or directory.


Example: ---A----"""

    text_mode_bits = fields.Str(
        data_key="text_mode_bits",
    )
    r""" Specifies the textual format of mode bits on this file or directory.


Example: rwxrwxrwx"""

    user_id = fields.Str(
        data_key="user_id",
    )
    r""" Specifies user ID of this file or directory.


Example: 10"""

    @property
    def resource(self):
        return FileDirectorySecurity

    gettable_fields = [
        "acls",
        "control_flags",
        "dos_attributes",
        "effective_style",
        "group",
        "group_id",
        "inode",
        "mode_bits",
        "owner",
        "security_style",
        "text_dos_attr",
        "text_mode_bits",
        "user_id",
    ]
    """acls,control_flags,dos_attributes,effective_style,group,group_id,inode,mode_bits,owner,security_style,text_dos_attr,text_mode_bits,user_id,"""

    patchable_fields = [
        "control_flags",
        "group",
        "owner",
    ]
    """control_flags,group,owner,"""

    postable_fields = [
        "access_control",
        "acls",
        "control_flags",
        "group",
        "ignore_paths",
        "owner",
        "propagation_mode",
    ]
    """access_control,acls,control_flags,group,ignore_paths,owner,propagation_mode,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in FileDirectorySecurity.get_collection(fields=field)]
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
            raise NetAppRestError("FileDirectorySecurity modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class FileDirectorySecurity(Resource):
    r""" Manages New Technology File System (NTFS) security and NTFS audit policies. """

    _schema = FileDirectorySecuritySchema
    _path = "/api/protocols/file-security/permissions/{svm[uuid]}/{file_directory_security[path]}"
    _keys = ["svm.uuid", "file_directory_security.path"]






    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves  file permissions
### Related ONTAP commands
* `vserver security file-directory show`

### Learn more
* [`DOC /protocols/file-security/permissions/{svm.uuid}/{path}`](#docs-NAS-protocols_file-security_permissions_{svm.uuid}_{path})"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="file directory security show")
        def file_directory_security_show(
            path,
            svm_uuid,
            access_control: Choices.define(_get_field_list("access_control"), cache_choices=True, inexact=True)=None,
            control_flags: Choices.define(_get_field_list("control_flags"), cache_choices=True, inexact=True)=None,
            dos_attributes: Choices.define(_get_field_list("dos_attributes"), cache_choices=True, inexact=True)=None,
            effective_style: Choices.define(_get_field_list("effective_style"), cache_choices=True, inexact=True)=None,
            group: Choices.define(_get_field_list("group"), cache_choices=True, inexact=True)=None,
            group_id: Choices.define(_get_field_list("group_id"), cache_choices=True, inexact=True)=None,
            ignore_paths: Choices.define(_get_field_list("ignore_paths"), cache_choices=True, inexact=True)=None,
            inode: Choices.define(_get_field_list("inode"), cache_choices=True, inexact=True)=None,
            mode_bits: Choices.define(_get_field_list("mode_bits"), cache_choices=True, inexact=True)=None,
            owner: Choices.define(_get_field_list("owner"), cache_choices=True, inexact=True)=None,
            propagation_mode: Choices.define(_get_field_list("propagation_mode"), cache_choices=True, inexact=True)=None,
            security_style: Choices.define(_get_field_list("security_style"), cache_choices=True, inexact=True)=None,
            text_dos_attr: Choices.define(_get_field_list("text_dos_attr"), cache_choices=True, inexact=True)=None,
            text_mode_bits: Choices.define(_get_field_list("text_mode_bits"), cache_choices=True, inexact=True)=None,
            user_id: Choices.define(_get_field_list("user_id"), cache_choices=True, inexact=True)=None,
            fields: List[str] = None,
        ) -> ResourceTable:
            """Fetch a single FileDirectorySecurity resource

            Args:
                access_control: An Access Control Level specifies the access control of the task to be applied. Valid values are \"file-directory\" or \"Storage-Level Access Guard (SLAG)\". SLAG is used to apply the specified security descriptors with the task for the volume or qtree. Otherwise, the security descriptors are applied on files and directories at the specified path. The value slag is not supported on FlexGroups volumes. The default value is \"file-directory\". 
                control_flags: Specifies the control flags in the SD. It is a Hexadecimal Value. 
                dos_attributes: Specifies the file attributes on this file or directory. 
                effective_style: Specifies the effective style of the SD. The following values are supported: * unix - UNIX style * ntfs - NTFS style * mixed - Mixed style * unified - Unified style 
                group: Specifies the owner's primary group. You can specify the owner group using either a group name or SID. 
                group_id: Specifies group ID on this file or directory. 
                ignore_paths: Specifies that permissions on this file or directory cannot be replaced. 
                inode: Specifies the File Inode number. 
                mode_bits: Specifies the mode bits on this file or directory. 
                owner: Specifies the owner of the SD. You can specify the owner using either a user name or security identifier (SID). The owner of the SD can modify the permissions on the file (or folder) or files (or folders) to which the SD is applied and can give other users the right to take ownership of the object or objects to which the SD is applied. 
                propagation_mode: Specifies how to propagate security settings to child subfolders and files. This setting determines how child files/folders contained within a parent folder inherit access control and audit information from the parent folder. The available values are: * propagate    - propagate inheritable permissions to all subfolders and files * replace      - replace existing permissions on all subfolders and files with inheritable permissions 
                security_style: Specifies the security style of the SD. The following values are supported: * unix - UNIX style * ntfs - NTFS style * mixed - Mixed style * unified - Unified style 
                text_dos_attr: Specifies the textual format of file attributes on this file or directory. 
                text_mode_bits: Specifies the textual format of mode bits on this file or directory. 
                user_id: Specifies user ID of this file or directory. 
            """

            kwargs = {}
            if access_control is not None:
                kwargs["access_control"] = access_control
            if control_flags is not None:
                kwargs["control_flags"] = control_flags
            if dos_attributes is not None:
                kwargs["dos_attributes"] = dos_attributes
            if effective_style is not None:
                kwargs["effective_style"] = effective_style
            if group is not None:
                kwargs["group"] = group
            if group_id is not None:
                kwargs["group_id"] = group_id
            if ignore_paths is not None:
                kwargs["ignore_paths"] = ignore_paths
            if inode is not None:
                kwargs["inode"] = inode
            if mode_bits is not None:
                kwargs["mode_bits"] = mode_bits
            if owner is not None:
                kwargs["owner"] = owner
            if propagation_mode is not None:
                kwargs["propagation_mode"] = propagation_mode
            if security_style is not None:
                kwargs["security_style"] = security_style
            if text_dos_attr is not None:
                kwargs["text_dos_attr"] = text_dos_attr
            if text_mode_bits is not None:
                kwargs["text_mode_bits"] = text_mode_bits
            if user_id is not None:
                kwargs["user_id"] = user_id
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            resource = FileDirectorySecurity(
                path,
                svm_uuid,
                **kwargs
            )
            resource.get()
            return [resource]

    def post(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Applies an SD  to the given path.
You must keep the following points in mind while using these endpoints:
* Either SLAG ACL/s or file-directory ACL/s can be configured in one API call. Both cannot be configured in the same API call.
* SLAG applies to all files and/or directories in a volume hence, inheritance is not required to be propagated.
* Set access_control field to slag while configuring SLAG ACLs.
* Set access_control field to file_directory while configuring file-directory ACLs. By Default access_control field is set to file_directory.
* For SLAG, valid apply_to combinations are "this-folder, sub-folders", "files", "this-folder, sub-folders, files".
### Related ONTAP commands
* `vserver security file-directory ntfs create`
* `vserver security file-directory ntfs dacl add`
* `vserver security file-directory ntfs sacl add`
* `vserver security file-directory policy create`
* `vserver security file-directory policy task add`
* `vserver security file-directory apply`

### Learn more
* [`DOC /protocols/file-security/permissions/{svm.uuid}/{path}`](#docs-NAS-protocols_file-security_permissions_{svm.uuid}_{path})"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="file directory security create")
        async def file_directory_security_create(
            path,
            svm_uuid,
            access_control: str = None,
            acls: dict = None,
            control_flags: str = None,
            dos_attributes: str = None,
            effective_style: str = None,
            group: str = None,
            group_id: str = None,
            ignore_paths: dict = None,
            inode: Size = None,
            mode_bits: Size = None,
            owner: str = None,
            propagation_mode: str = None,
            security_style: str = None,
            text_dos_attr: str = None,
            text_mode_bits: str = None,
            user_id: str = None,
        ) -> ResourceTable:
            """Create an instance of a FileDirectorySecurity resource

            Args:
                access_control: An Access Control Level specifies the access control of the task to be applied. Valid values are \"file-directory\" or \"Storage-Level Access Guard (SLAG)\". SLAG is used to apply the specified security descriptors with the task for the volume or qtree. Otherwise, the security descriptors are applied on files and directories at the specified path. The value slag is not supported on FlexGroups volumes. The default value is \"file-directory\". 
                acls: A discretionary access security list (DACL) identifies the trustees that are allowed or denied access to a securable object. When a process tries to access a securable object, the system checks the access control entries (ACEs) in the object's DACL to determine whether to grant access to it. 
                control_flags: Specifies the control flags in the SD. It is a Hexadecimal Value. 
                dos_attributes: Specifies the file attributes on this file or directory. 
                effective_style: Specifies the effective style of the SD. The following values are supported: * unix - UNIX style * ntfs - NTFS style * mixed - Mixed style * unified - Unified style 
                group: Specifies the owner's primary group. You can specify the owner group using either a group name or SID. 
                group_id: Specifies group ID on this file or directory. 
                ignore_paths: Specifies that permissions on this file or directory cannot be replaced. 
                inode: Specifies the File Inode number. 
                mode_bits: Specifies the mode bits on this file or directory. 
                owner: Specifies the owner of the SD. You can specify the owner using either a user name or security identifier (SID). The owner of the SD can modify the permissions on the file (or folder) or files (or folders) to which the SD is applied and can give other users the right to take ownership of the object or objects to which the SD is applied. 
                propagation_mode: Specifies how to propagate security settings to child subfolders and files. This setting determines how child files/folders contained within a parent folder inherit access control and audit information from the parent folder. The available values are: * propagate    - propagate inheritable permissions to all subfolders and files * replace      - replace existing permissions on all subfolders and files with inheritable permissions 
                security_style: Specifies the security style of the SD. The following values are supported: * unix - UNIX style * ntfs - NTFS style * mixed - Mixed style * unified - Unified style 
                text_dos_attr: Specifies the textual format of file attributes on this file or directory. 
                text_mode_bits: Specifies the textual format of mode bits on this file or directory. 
                user_id: Specifies user ID of this file or directory. 
            """

            kwargs = {}
            if access_control is not None:
                kwargs["access_control"] = access_control
            if acls is not None:
                kwargs["acls"] = acls
            if control_flags is not None:
                kwargs["control_flags"] = control_flags
            if dos_attributes is not None:
                kwargs["dos_attributes"] = dos_attributes
            if effective_style is not None:
                kwargs["effective_style"] = effective_style
            if group is not None:
                kwargs["group"] = group
            if group_id is not None:
                kwargs["group_id"] = group_id
            if ignore_paths is not None:
                kwargs["ignore_paths"] = ignore_paths
            if inode is not None:
                kwargs["inode"] = inode
            if mode_bits is not None:
                kwargs["mode_bits"] = mode_bits
            if owner is not None:
                kwargs["owner"] = owner
            if propagation_mode is not None:
                kwargs["propagation_mode"] = propagation_mode
            if security_style is not None:
                kwargs["security_style"] = security_style
            if text_dos_attr is not None:
                kwargs["text_dos_attr"] = text_dos_attr
            if text_mode_bits is not None:
                kwargs["text_mode_bits"] = text_mode_bits
            if user_id is not None:
                kwargs["user_id"] = user_id

            resource = FileDirectorySecurity(
                path,
                svm_uuid,
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create FileDirectorySecurity: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates SD specific Information. For example, owner, group and control-flags. SD specific information of SLAG ACLs is not modifiable.
### Related ONTAP commands
* `vserver security file-directory ntfs modify`

### Learn more
* [`DOC /protocols/file-security/permissions/{svm.uuid}/{path}`](#docs-NAS-protocols_file-security_permissions_{svm.uuid}_{path})"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="file directory security modify")
        async def file_directory_security_modify(
            path,
            svm_uuid,
            access_control: str = None,
            query_access_control: str = None,
            control_flags: str = None,
            query_control_flags: str = None,
            dos_attributes: str = None,
            query_dos_attributes: str = None,
            effective_style: str = None,
            query_effective_style: str = None,
            group: str = None,
            query_group: str = None,
            group_id: str = None,
            query_group_id: str = None,
            ignore_paths: dict = None,
            query_ignore_paths: dict = None,
            inode: Size = None,
            query_inode: Size = None,
            mode_bits: Size = None,
            query_mode_bits: Size = None,
            owner: str = None,
            query_owner: str = None,
            propagation_mode: str = None,
            query_propagation_mode: str = None,
            security_style: str = None,
            query_security_style: str = None,
            text_dos_attr: str = None,
            query_text_dos_attr: str = None,
            text_mode_bits: str = None,
            query_text_mode_bits: str = None,
            user_id: str = None,
            query_user_id: str = None,
        ) -> ResourceTable:
            """Modify an instance of a FileDirectorySecurity resource

            Args:
                access_control: An Access Control Level specifies the access control of the task to be applied. Valid values are \"file-directory\" or \"Storage-Level Access Guard (SLAG)\". SLAG is used to apply the specified security descriptors with the task for the volume or qtree. Otherwise, the security descriptors are applied on files and directories at the specified path. The value slag is not supported on FlexGroups volumes. The default value is \"file-directory\". 
                query_access_control: An Access Control Level specifies the access control of the task to be applied. Valid values are \"file-directory\" or \"Storage-Level Access Guard (SLAG)\". SLAG is used to apply the specified security descriptors with the task for the volume or qtree. Otherwise, the security descriptors are applied on files and directories at the specified path. The value slag is not supported on FlexGroups volumes. The default value is \"file-directory\". 
                control_flags: Specifies the control flags in the SD. It is a Hexadecimal Value. 
                query_control_flags: Specifies the control flags in the SD. It is a Hexadecimal Value. 
                dos_attributes: Specifies the file attributes on this file or directory. 
                query_dos_attributes: Specifies the file attributes on this file or directory. 
                effective_style: Specifies the effective style of the SD. The following values are supported: * unix - UNIX style * ntfs - NTFS style * mixed - Mixed style * unified - Unified style 
                query_effective_style: Specifies the effective style of the SD. The following values are supported: * unix - UNIX style * ntfs - NTFS style * mixed - Mixed style * unified - Unified style 
                group: Specifies the owner's primary group. You can specify the owner group using either a group name or SID. 
                query_group: Specifies the owner's primary group. You can specify the owner group using either a group name or SID. 
                group_id: Specifies group ID on this file or directory. 
                query_group_id: Specifies group ID on this file or directory. 
                ignore_paths: Specifies that permissions on this file or directory cannot be replaced. 
                query_ignore_paths: Specifies that permissions on this file or directory cannot be replaced. 
                inode: Specifies the File Inode number. 
                query_inode: Specifies the File Inode number. 
                mode_bits: Specifies the mode bits on this file or directory. 
                query_mode_bits: Specifies the mode bits on this file or directory. 
                owner: Specifies the owner of the SD. You can specify the owner using either a user name or security identifier (SID). The owner of the SD can modify the permissions on the file (or folder) or files (or folders) to which the SD is applied and can give other users the right to take ownership of the object or objects to which the SD is applied. 
                query_owner: Specifies the owner of the SD. You can specify the owner using either a user name or security identifier (SID). The owner of the SD can modify the permissions on the file (or folder) or files (or folders) to which the SD is applied and can give other users the right to take ownership of the object or objects to which the SD is applied. 
                propagation_mode: Specifies how to propagate security settings to child subfolders and files. This setting determines how child files/folders contained within a parent folder inherit access control and audit information from the parent folder. The available values are: * propagate    - propagate inheritable permissions to all subfolders and files * replace      - replace existing permissions on all subfolders and files with inheritable permissions 
                query_propagation_mode: Specifies how to propagate security settings to child subfolders and files. This setting determines how child files/folders contained within a parent folder inherit access control and audit information from the parent folder. The available values are: * propagate    - propagate inheritable permissions to all subfolders and files * replace      - replace existing permissions on all subfolders and files with inheritable permissions 
                security_style: Specifies the security style of the SD. The following values are supported: * unix - UNIX style * ntfs - NTFS style * mixed - Mixed style * unified - Unified style 
                query_security_style: Specifies the security style of the SD. The following values are supported: * unix - UNIX style * ntfs - NTFS style * mixed - Mixed style * unified - Unified style 
                text_dos_attr: Specifies the textual format of file attributes on this file or directory. 
                query_text_dos_attr: Specifies the textual format of file attributes on this file or directory. 
                text_mode_bits: Specifies the textual format of mode bits on this file or directory. 
                query_text_mode_bits: Specifies the textual format of mode bits on this file or directory. 
                user_id: Specifies user ID of this file or directory. 
                query_user_id: Specifies user ID of this file or directory. 
            """

            kwargs = {}
            changes = {}
            if query_access_control is not None:
                kwargs["access_control"] = query_access_control
            if query_control_flags is not None:
                kwargs["control_flags"] = query_control_flags
            if query_dos_attributes is not None:
                kwargs["dos_attributes"] = query_dos_attributes
            if query_effective_style is not None:
                kwargs["effective_style"] = query_effective_style
            if query_group is not None:
                kwargs["group"] = query_group
            if query_group_id is not None:
                kwargs["group_id"] = query_group_id
            if query_ignore_paths is not None:
                kwargs["ignore_paths"] = query_ignore_paths
            if query_inode is not None:
                kwargs["inode"] = query_inode
            if query_mode_bits is not None:
                kwargs["mode_bits"] = query_mode_bits
            if query_owner is not None:
                kwargs["owner"] = query_owner
            if query_propagation_mode is not None:
                kwargs["propagation_mode"] = query_propagation_mode
            if query_security_style is not None:
                kwargs["security_style"] = query_security_style
            if query_text_dos_attr is not None:
                kwargs["text_dos_attr"] = query_text_dos_attr
            if query_text_mode_bits is not None:
                kwargs["text_mode_bits"] = query_text_mode_bits
            if query_user_id is not None:
                kwargs["user_id"] = query_user_id

            if access_control is not None:
                changes["access_control"] = access_control
            if control_flags is not None:
                changes["control_flags"] = control_flags
            if dos_attributes is not None:
                changes["dos_attributes"] = dos_attributes
            if effective_style is not None:
                changes["effective_style"] = effective_style
            if group is not None:
                changes["group"] = group
            if group_id is not None:
                changes["group_id"] = group_id
            if ignore_paths is not None:
                changes["ignore_paths"] = ignore_paths
            if inode is not None:
                changes["inode"] = inode
            if mode_bits is not None:
                changes["mode_bits"] = mode_bits
            if owner is not None:
                changes["owner"] = owner
            if propagation_mode is not None:
                changes["propagation_mode"] = propagation_mode
            if security_style is not None:
                changes["security_style"] = security_style
            if text_dos_attr is not None:
                changes["text_dos_attr"] = text_dos_attr
            if text_mode_bits is not None:
                changes["text_mode_bits"] = text_mode_bits
            if user_id is not None:
                changes["user_id"] = user_id

            if hasattr(FileDirectorySecurity, "find"):
                resource = FileDirectorySecurity.find(
                    path,
                    svm_uuid,
                    **kwargs
                )
            else:
                resource = FileDirectorySecurity(path,svm_uuid,)
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify FileDirectorySecurity: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Remove all SLAG ACLs for specified path. Bulk deletion is supported only for SLAG
# Related ONTAP Commands
* `vserver security file-directory remove-slag`

### Learn more
* [`DOC /protocols/file-security/permissions/{svm.uuid}/{path}`](#docs-NAS-protocols_file-security_permissions_{svm.uuid}_{path})"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="file directory security delete")
        async def file_directory_security_delete(
            path,
            svm_uuid,
            access_control: str = None,
            control_flags: str = None,
            dos_attributes: str = None,
            effective_style: str = None,
            group: str = None,
            group_id: str = None,
            ignore_paths: dict = None,
            inode: Size = None,
            mode_bits: Size = None,
            owner: str = None,
            propagation_mode: str = None,
            security_style: str = None,
            text_dos_attr: str = None,
            text_mode_bits: str = None,
            user_id: str = None,
        ) -> None:
            """Delete an instance of a FileDirectorySecurity resource

            Args:
                access_control: An Access Control Level specifies the access control of the task to be applied. Valid values are \"file-directory\" or \"Storage-Level Access Guard (SLAG)\". SLAG is used to apply the specified security descriptors with the task for the volume or qtree. Otherwise, the security descriptors are applied on files and directories at the specified path. The value slag is not supported on FlexGroups volumes. The default value is \"file-directory\". 
                control_flags: Specifies the control flags in the SD. It is a Hexadecimal Value. 
                dos_attributes: Specifies the file attributes on this file or directory. 
                effective_style: Specifies the effective style of the SD. The following values are supported: * unix - UNIX style * ntfs - NTFS style * mixed - Mixed style * unified - Unified style 
                group: Specifies the owner's primary group. You can specify the owner group using either a group name or SID. 
                group_id: Specifies group ID on this file or directory. 
                ignore_paths: Specifies that permissions on this file or directory cannot be replaced. 
                inode: Specifies the File Inode number. 
                mode_bits: Specifies the mode bits on this file or directory. 
                owner: Specifies the owner of the SD. You can specify the owner using either a user name or security identifier (SID). The owner of the SD can modify the permissions on the file (or folder) or files (or folders) to which the SD is applied and can give other users the right to take ownership of the object or objects to which the SD is applied. 
                propagation_mode: Specifies how to propagate security settings to child subfolders and files. This setting determines how child files/folders contained within a parent folder inherit access control and audit information from the parent folder. The available values are: * propagate    - propagate inheritable permissions to all subfolders and files * replace      - replace existing permissions on all subfolders and files with inheritable permissions 
                security_style: Specifies the security style of the SD. The following values are supported: * unix - UNIX style * ntfs - NTFS style * mixed - Mixed style * unified - Unified style 
                text_dos_attr: Specifies the textual format of file attributes on this file or directory. 
                text_mode_bits: Specifies the textual format of mode bits on this file or directory. 
                user_id: Specifies user ID of this file or directory. 
            """

            kwargs = {}
            if access_control is not None:
                kwargs["access_control"] = access_control
            if control_flags is not None:
                kwargs["control_flags"] = control_flags
            if dos_attributes is not None:
                kwargs["dos_attributes"] = dos_attributes
            if effective_style is not None:
                kwargs["effective_style"] = effective_style
            if group is not None:
                kwargs["group"] = group
            if group_id is not None:
                kwargs["group_id"] = group_id
            if ignore_paths is not None:
                kwargs["ignore_paths"] = ignore_paths
            if inode is not None:
                kwargs["inode"] = inode
            if mode_bits is not None:
                kwargs["mode_bits"] = mode_bits
            if owner is not None:
                kwargs["owner"] = owner
            if propagation_mode is not None:
                kwargs["propagation_mode"] = propagation_mode
            if security_style is not None:
                kwargs["security_style"] = security_style
            if text_dos_attr is not None:
                kwargs["text_dos_attr"] = text_dos_attr
            if text_mode_bits is not None:
                kwargs["text_mode_bits"] = text_mode_bits
            if user_id is not None:
                kwargs["user_id"] = user_id

            if hasattr(FileDirectorySecurity, "find"):
                resource = FileDirectorySecurity.find(
                    path,
                    svm_uuid,
                    **kwargs
                )
            else:
                resource = FileDirectorySecurity(path,svm_uuid,)
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete FileDirectorySecurity: %s" % err)


