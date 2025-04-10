cwlVersion: v1.2
class: Workflow

inputs:
  settings:
    type: File

outputs: {}

steps:
  setup:
    run: setup_ml_repo.cwl
    in:
      settings: settings
    out: [repo_setup_output]

  install:
    run: install_requirements.cwl
    in:
      settings: settings
    out: [requirements_output]
