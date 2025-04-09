cwlVersion: v1.2
class: CommandLineTool
baseCommand: ["python3", "/home/leonardo/AirflowDemo/StreamFlow/step/new_project.py"]

inputs:
  previous_output:
    type: File
    inputBinding:
      position: 1
outputs: []
