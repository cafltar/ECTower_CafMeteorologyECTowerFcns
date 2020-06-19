### Purpose

Azure Function with EventGrid trigger that moves new blobs in the "transient" zone of the CAF LTAR Data Lake to the proper subfolder of the CafMeteorologyECTower project folder in the "raw" zone of the CAF LTAR Data Lake. Function also logs transaction in "_log" folder of the project folder.

### Assumptions

* Azure Function App exists for this to deploy to and is runtime version ~3
* The Azure Function App has proper permissions (through Managed Identity) for data lake: read, write, execute
* Caf.Etl v4.3.2
* Caf.SciEtl v0.1.0-beta.8
* The following are defined in the Application Settings: 
  * FUNCTION_OBJECT_ID: ID of the Function App
  * OUTPUT_CONTAINER: Name of the container to copy to (e.g. "raw")
  * PROJECT_ID: The project ID for the folder within the OUTPUT_CONTAINER to copy to (e.g. "CafMeteorologyECTower")
  * DATALAKE_ENDPOINT: The endpoint of the datalake (e.g. "https://XXX.dfs.core.windows.net")

### License

As a work of the United States government, this project is in the public domain within the United States.

Additionally, we waive copyright and related rights in the work worldwide through the CC0 1.0 Universal public domain dedication.
