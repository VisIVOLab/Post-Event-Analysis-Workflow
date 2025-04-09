cwlVersion: v1.2
class: CommandLineTool
baseCommand: ["python3", "/home/leonardo/AirflowDemo/StreamFlow/step/import_photos.py"]

inputs:
  previous_output:
    type: File
    inputBinding:
      position: 1
outputs: []
