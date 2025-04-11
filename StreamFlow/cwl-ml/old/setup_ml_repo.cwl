cwlVersion: v1.2
class: CommandLineTool
label: "Clone or update ML repository"
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
  repo_url:
    type: string

arguments:
  - valueFrom: |
        MOTHER_DIR="$(inputs.project_root_folder)/$(inputs.ml_folder)"
        REPO_URL="$(inputs.repo_url)"
        TARGET_DIR="$MOTHER_DIR/$(inputs.repo_name)"

        mkdir -p $MOTHER_DIR

        if [ -d "$TARGET_DIR/.git" ]; then
            echo "Repo already exists. Pulling latest changes..."
            cd "$TARGET_DIR"
            git pull
        else
            echo "Cloning repo for the first time..."
            git clone --depth 1 "$REPO_URL" "$TARGET_DIR"
        fi
    position: 1

# stdout: logs/setup_ml_repo.log
# stderr: logs/setup_ml_repo.err

outputs:
  setup_log:
    type: File
    outputBinding:
      glob: logs/setup_ml_repo.log
  setup_err:
    type: File
    outputBinding:
      glob: logs/setup_ml_repo.err
