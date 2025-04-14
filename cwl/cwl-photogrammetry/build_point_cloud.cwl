cwlVersion: v1.0
class: CommandLineTool
baseCommand: ["python3", "/home/leonardo/AirflowDemo/cwl/step/build_point_cloud.py"]

inputs:
    previous_output:
        type: File
        inputBinding:
            position: 1
    config:
      type: File
      inputBinding:
        position: 1
    input_status:
        type: File
outputs: []