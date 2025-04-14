cwlVersion: v1.2
class: Workflow
label: "ML Workflow – solo setup repo con due config JSON"

inputs:
  default_args: File
  dagrun_cfg: File

outputs: {}

steps:
  extract_defaults:
    run:
      class: ExpressionTool
      inputs:
        default_args: File
      outputs:
        ml_folder: string
        repo_folder: string
        repo_url: string
      expression: >
        ${
          const fs = require('fs');
          const cfg = JSON.parse(fs.readFileSync(inputs.default_args.path, 'utf8'));
          return {
            ml_folder: cfg.ml_folder,
            repo_folder: cfg.repo_folder,
            repo_url: cfg.repo_url
          };
        }

  setup_ml_repo:
    run: setup_ml_repo.cwl
    in:
      ml_folder: extract_defaults/ml_folder
      repo_folder: extract_defaults/repo_folder
      repo_url: extract_defaults/repo_url
    out: []
