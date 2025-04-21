cwlVersion: v1.2
class: CommandLineTool
baseCommand: ["python3", "./cwl/step/match_align_photos.py"]

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
      glob: "match_and_align.done"

doc: Camera matching and camera align.