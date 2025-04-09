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
    out: [status]

  import_photos:
    run: import_photos.cwl
    in:
      previous_output: init_config/output_json
      input_status: new_project/status
    out: [status]

  match_and_align:
    run: match_and_align.cwl
    in:
      previous_output: init_config/output_json
      input_status: import_photos/status
    out: [status]


outputs:
  init_result:
    type: File
    outputSource: init_config/output_json