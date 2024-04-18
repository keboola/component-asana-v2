# Asana Extractor
Asana extractor allows you to extract details and status of your projects and tasks.

## To obtain an API Key ##
1. Login to [Asana](https://app.asana.com/)
2. Click on your account icon at the top right corner of the page
3. Click *My Profile Settings...*.
4. Click on *Apps* tab.
5. Click on *Manage Developer Apps*.
6. Click on *\+ Create New Personal Access Token*.

## Configuration
1. Token
2. Endpoints
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
3. Incremental Load
    - *Enable* : The component will start extraction from the last persisted extraction date and load results into respective tables incrementally. You can also override the start extraction date with the Date From Parameter
    - *Disable* : The component will extract `everything` from the respective endpoint and full load the responses into the respective output tables in Keboola storage.
4. Load options
    - *Date From* : Date from which data is downloaded (only affects Tasks endpoint). Either date in YYYY-MM-DD format or dateparser string i.e. 5 days ago, 1 month ago, yesterday, etc. You can also set this as last run, which will fetch data from the last run of the component. The component uses completed_since parameter which only return tasks that are either incomplete or that have been completed since this time.
5. Project IDs
    - Required when endpoint `Projects - User Defined` is selected
    - Please enter your values with comma delimiter.

    **Notes: If `Projects - User Defined` is selected, the component will NOT fetch data from `Projects` endpoint and `Archived Projects` endpoint.
