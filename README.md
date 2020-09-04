# Asana Extractor
Asana extractor allows you to extract dtails and status of your projects and tasks.

## To obtain an API Key ##
1. Login to [Asana](https://app.asana.com/)
2. Click on your account icon at the top right corner of the page
3. Click *My Profile Settings...*.
4. Click on *Apps* tab.
5. Click on *Manage Developer Apps*.
6. Click on *\+ Create New Personal Access Token*.

## Configuration
1. Token
2. Incremental Load
    - It persist the date from last component run which will be used for the next run. If user wants to add a new endpoint with the full history, please set this option as false first; else, with this option as true, the component will always pick up from where it left off.
3. Endpoints
    1. Users
    2. Users Details
    3. Projects
    4. Archived Projects
    5. Project Sections
    6. Project Tasks
    7. Project Task Details
    8. Project Tasks' Subtasks
    9. Project Tasks' Stories