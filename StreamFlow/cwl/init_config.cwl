cwlVersion: v1.2
class: CommandLineTool
baseCommand: ["python3", "/home/leonardo/AirflowDemo/StreamFlow/step/init_config.py"]

inputs:
  config:
    type: File
    inputBinding:
      position: 1
outputs:
  output_json:
    type: File
    outputBinding:
      glob: init_out.json

doc: Extracts project_path, image_path, num_gpus, and gpu_mask from a config file
  using Metashape.
