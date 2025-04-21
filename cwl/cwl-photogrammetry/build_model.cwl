cwlVersion: v1.2
class: CommandLineTool
baseCommand: ["python3", "./cwl/step/build_model.py"]

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

doc: Build 3D model.