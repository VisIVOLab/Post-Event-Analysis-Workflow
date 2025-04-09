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
  
  build_depth_maps:
    run: build_depth_maps.cwl
    in:
      previous_output: init_config/output_json
      input_status: match_and_align/status
    out: [status]

  build_point_cloud:
    run: build_point_cloud.cwl
    in:
      previous_output: init_config/output_json
      input_status: build_depth_maps/status
    out: []

  build_tiled:
    run: build_tiled.cwl
    in:
      previous_output: init_config/output_json
      input_status: build_depth_maps/status
    out: []


outputs:
  init_result:
    type: File
    outputSource: init_config/output_json