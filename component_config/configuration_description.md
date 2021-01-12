## Configuration Parameters
1. Token
2. Incremental Load
    - *Enable* : The component will start extraction from the last persisted extraction date and load results into respective tables incrementally.
    - *Disable* : The component will extract `everything` from the respective endpoint and full load the respones into the respective output tables in Keboola storage.
3. Endpoints
    1. Users
    2. Users Details
    3. Projects
    4. Archived Projects
    5. Project Sections
    6. Project Tasks
    7. Project Tasks Details
    8. Project Tasks Subtasks
    9. Project Tasks Stories