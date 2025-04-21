cwlVersion: v1.2
class: CommandLineTool
baseCommand: ["python3", "./cwl/step/build_depth_maps.py"]

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
outputs:
  status:
    type: File
    outputBinding:
      glob: "build_depth_maps.done"

doc: Build depth maps.