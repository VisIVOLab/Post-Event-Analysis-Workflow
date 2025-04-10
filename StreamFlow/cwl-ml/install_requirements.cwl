cwlVersion: v1.2
class: CommandLineTool
label: "Install Python requirements from repo"

baseCommand: [bash, -c]

requirements:
  InlineJavascriptRequirement: {}

inputs:
  project_root_folder:
    type: string
  ml_folder:
    type: string  
  repo_name:
    type: string

arguments:
  - position: 1
    valueFrom: |
      pip install -r \
      $(inputs.project_root_folder)/$(inputs.ml_folder)/$(inputs.repo_name)/requirements.txt

stdout: install_requirements.log
stderr: install_requirements.err


outputs: {}
