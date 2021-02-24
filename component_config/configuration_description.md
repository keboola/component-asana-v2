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
    - *Enable* : The component will start extraction from the last persisted extraction date and load results into respective tables incrementally.
    - *Disable* : The component will extract `everything` from the respective endpoint and full load the respones into the respective output tables in Keboola storage.
3. Endpoints
    1. Users
    2. Users Details
    3. Projects
    4. Projects - User Defined
    5. Archived Projects
    6. Project Sections
    7. Project Tasks
    8. Project Tasks Details
    9. Project Tasks Subtasks
    10. Project Tasks Stories

    **Notes: If `Projects - User Defined` is selected, the component will NOT fetch data from `Projects` endpoint and `Archived Projects` endpoint.

4. Project IDs
    - Required when endpoint `Projects - User Defined` is selected
    - Please enter your values with comma delimiter.