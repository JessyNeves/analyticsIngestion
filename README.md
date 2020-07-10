# analyticsIngestion [Under Construction]
Built using macOs : PyCharm Community Version
Python 3.7 


This python project implements a simple pipeline that imports Google
Analytics sample data.
When run, the user will be prompted. Insert the date you wish to import,
consecutive data arriving to the pipeline will be appended to the .parquet
file created on first run of this pipeline.

A ControlTable is written to root folder as a .csv file. This file
will be read each time you run the pipeline to check if you have already
imported the date you are passing to the pipeline. If so, the pipeline
throws an error.
This file is also used to control if the import was sucessfully, both
from the API ("ImportStatus should be OK") and if the data was sucessfully
written to the parquet file ("ParquetWritten should be OK").

If data is sucessfully fetched, a .parquet file will be written to the root
folder. 

- BigQuery API is being used and needs to be installed (i.e via pip)
- Pandas is being used.
- NumPy is being used.
