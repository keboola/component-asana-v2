from mapping_parser import MappingParser
import json


endpoint = 'projects_tasks_details'
data_in = {
    "data": {
        "gid": "12345",
        "resource_type": "task",
        "name": "Buy catnip",
        "approval_status": "pending",
        "completed": False,
        "completed_at": "2012-02-22T02:06:58.147Z",
        "completed_by": {
            "gid": "12345",
            "resource_type": "user",
            "name": "Greg Sanchez"
        },
        "created_at": "2012-02-22T02:06:58.147Z",
        "dependencies": [
            {
                "gid": "12345",
                "resource_type": "task"
            }
        ],
        "dependents": [
            {
                "gid": "12345",
                "resource_type": "task"
            }
        ],
        "due_at": "2019-09-15T02:06:58.147Z",
        "due_on": "2019-09-15",
        "external": {
            "data": "A blob of information",
            "gid": "my_gid"
        },
        "hearted": True,
        "hearts": [
            {
                "gid": "12345",
                "user": {
                    "gid": "12345",
                    "resource_type": "user",
                    "name": "Greg Sanchez"
                }
            }
        ],
        "html_notes": "<body>Mittens <em>really</em> likes the stuff from Humboldt.</body>",
        "is_rendered_as_separator": False,
        "liked": True,
        "likes": [
            {
                "gid": "12345",
                "user": {
                    "gid": "12345",
                    "resource_type": "user",
                    "name": "Greg Sanchez"
                }
            }
        ],
        "memberships": [
            {
                "project": {
                    "gid": "12345",
                    "resource_type": "project",
                    "name": "Stuff to buy"
                },
                "section": {
                    "gid": "12345",
                    "resource_type": "section",
                    "name": "Next Actions"
                }
            }
        ],
        "modified_at": "2012-02-22T02:06:58.147Z",
        "notes": "Mittens really likes the stuff from Humboldt.",
        "num_hearts": 5,
        "num_likes": 5,
        "num_subtasks": 3,
        "resource_subtype": "default_task",
        "start_on": "2019-09-14",
        "assignee": {
            "gid": "12345",
            "resource_type": "user",
            "name": "Greg Sanchez"
        },
        "custom_fields": [
            {
                "gid": "12345",
                "resource_type": "custom_field",
                "created_by": {
                    "gid": "12345",
                    "resource_type": "user",
                    "name": "Greg Sanchez"
                },
                "currency_code": "EUR",
                "custom_label": "gold pieces",
                "custom_label_position": "suffix",
                "description": "Development team priority",
                "display_value": "blue",
                "enabled": True,
                "enum_options": [
                    {
                        "gid": "12345",
                        "resource_type": "enum_option",
                        "color": "blue",
                        "enabled": True,
                        "name": "Low"
                    }
                ],
                "enum_value": {
                    "gid": "12345",
                    "resource_type": "enum_option",
                    "color": "blue",
                    "enabled": True,
                    "name": "Low"
                },
                "format": "custom",
                "has_notifications_enabled": True,
                "is_global_to_workspace": True,
                "name": "Status",
                "number_value": 5.2,
                "precision": 2,
                "resource_subtype": "text",
                "text_value": "Some Value",
                "type": "text"
            },
            {
                "gid": "1234567890",
                "resource_type": "custom_field",
                "created_by": {
                    "gid": "12345",
                    "resource_type": "user",
                    "name": "Greg Sanchez"
                },
                "currency_code": "EUR",
                "custom_label": "gold pieces",
                "custom_label_position": "suffix",
                "description": "Development team priority",
                "display_value": "blue",
                "enabled": True,
                "enum_options": [
                    {
                        "gid": "12345",
                        "resource_type": "enum_option",
                        "color": "blue",
                        "enabled": True,
                        "name": "Low"
                    }
                ],
                "enum_value": {
                    "gid": "12345",
                    "resource_type": "enum_option",
                    "color": "blue",
                    "enabled": True,
                    "name": "Low"
                },
                "format": "custom",
                "has_notifications_enabled": True,
                "is_global_to_workspace": True,
                "name": "custom_field",
                "number_value": 5.2,
                "precision": 2,
                "resource_subtype": "text",
                "text_value": "Some Value",
                "type": "text"
            }
        ],
        "followers": [
            {
                "gid": "12345",
                "resource_type": "user",
                "name": "Greg Sanchez"
            }
        ],
        "parent": {
            "gid": "12345",
            "resource_type": "task",
            "name": "Bug Task"
        },
        "permalink_url": "https://app.asana.com/0/resource/123456789/list",
        "projects": [
            {
                "gid": "12345",
                "resource_type": "project",
                "name": "Stuff to buy"
            }
        ],
        "tags": [
            {
                "gid": "59746",
                "name": "Grade A"
            }
        ],
        "workspace": {
            "gid": "12345",
            "resource_type": "workspace",
            "name": "My Company Workspace"
        }
    }
}

with open('src/endpoint_mappings.json', 'r') as m:
    MAPPINGS = json.load(m)

REQUEST_MAP = {
    'workspaces': {
        'endpoint': 'workspaces',
        'mapping': 'workspaces'},
    'users': {
        'endpoint': 'workspaces/{workspaces_id}/users',
        'required': 'workspaces', 'mapping': 'users'},
    'users_details': {
        'endpoint': 'users/{users_id}',
        'required': 'users', 'mapping': 'users_details'},
    'user_defined_projects': {
        'endpoint': 'projects/{projects_id}',
        'required': 'projects', 'mapping': 'projects_details'},
    'projects': {
        'endpoint': 'workspaces/{workspaces_id}/projects',
        'required': 'workspaces', 'mapping': 'projects'},
    'projects_details': {
        'endpoint': 'projects/{projects_id}',
        'required': 'projects', 'mapping': 'projects_details'},
    'projects_sections': {
        'endpoint': 'projects/{projects_id}/sections',
        'required': 'projects', 'mapping': 'sections'},
    'projects_tasks': {
        'endpoint': 'projects/{projects_id}/tasks',
        'required': 'projects', 'mapping': 'tasks'},
    'projects_tasks_details': {
        'endpoint': 'tasks/{projects_tasks_id}',
        'required': 'projects_tasks', 'mapping': 'task_details'},
    'projects_tasks_subtasks': {
        'endpoint': 'tasks/{projects_tasks_id}/subtasks',
        'required': 'projects_tasks', 'mapping': 'task_subtasks'},
    'projects_tasks_stories': {
        'endpoint': 'tasks/{projects_tasks_id}/stories',
        'required': 'projects_tasks', 'mapping': 'task_stories'}
}

endpoint_mapping = MAPPINGS[REQUEST_MAP[endpoint]['mapping']]

MappingParser(
    destination='/data/out/tables/',
    endpoint=REQUEST_MAP[endpoint]['mapping'],
    endpoint_data=data_in['data'],
    mapping=endpoint_mapping,
    incremental=None,
    parent_key='test'
)
