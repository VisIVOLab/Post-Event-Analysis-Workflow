mlcwlVersion: v1.2
class: CommandLineTool
label: "Clone or update ML repository"
baseCommand: [bash, "-c"]

inputs:
  ml_folder:
    type: string
    inputBinding:
      position: 1
  repo_folder:
    type: string
    inputBinding:
      position: 2
  repo_url:
    type: string
    inputBinding:
      position: 3

arguments:
  - |
    ML_DIR="$1"
    REPO_DIR="$2"
    REPO_URL="$3"

    mkdir -p "$ML_DIR"

    if [ -d "$REPO_DIR/.git" ]; then
        echo "Repo already exists. Pulling latest changes..."
        cd "$REPO_DIR"
        git pull
    else
        echo "Cloning repo for the first time..."
        git clone --depth 1 "$REPO_URL" "$REPO_DIR"
    fi

outputs: []
