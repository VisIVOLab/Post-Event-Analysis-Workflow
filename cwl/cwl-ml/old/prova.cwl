cwlVersion: v1.2
class: CommandLineTool
label: "prova"
baseCommand: echo

requirements:
  InlineJavascriptRequirement: {}

inputs:
  project_root_folder:
    type: string
  repo_url:
    type: string
  repo_folder:
    type: string

arguments:
  - valueFrom: |
      ciao $(inputs.project_root_folder)/$(inputs.repo_folder)
    position: 1

stdout: setup_ml_repo.log

outputs: {}
