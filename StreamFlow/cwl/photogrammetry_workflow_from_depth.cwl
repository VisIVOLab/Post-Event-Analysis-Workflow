cwlVersion: v1.2
class: Workflow

inputs:
  config_file: File

steps:
  init_config:
    run: init_config.cwl
    in:
      config: config_file
    out: [output_json]
  
  new_project:
    run: new_project.cwl
    in:
      previous_output: init_config/output_json
    out: []


outputs:
  init_result:
    type: File
    outputSource: init_config/output_json