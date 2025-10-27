# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "188ddb59-62fb-4825-a5cd-eba7f6c85267",
# META       "default_lakehouse_name": "demo_source",
# META       "default_lakehouse_workspace_id": "82a01462-d2aa-47b6-a126-a799cb95d2f1",
# META       "known_lakehouses": [
# META         {
# META           "id": "188ddb59-62fb-4825-a5cd-eba7f6c85267"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
from datetime import datetime

timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
files = mssparkutils.fs.ls("Files/incoming")

log_msg = f"✅ Trigger fired at {timestamp}\nDetected files:"
for f in files:
    log_msg += f"\n - {f.path}"

# Write log file to Lakehouse
log_path = f"Files/logs/trigger_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
mssparkutils.fs.put(log_path, log_msg, True)

print(log_msg)
print(f"\nLog written to: {log_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
