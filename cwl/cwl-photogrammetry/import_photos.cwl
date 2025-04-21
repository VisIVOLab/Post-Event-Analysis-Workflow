cwlVersion: v1.2
class: CommandLineTool
baseCommand: ["python3", "./cwl/step/import_photos.py"]

inputs:
  previous_output:
    type: File
    inputBinding:
      position: 1
  input_status:
    type: File
outputs:
  status:
    type: File
    outputBinding:
      glob: "import_photos.done"

doc: Set cameras on first Metashape chunk.