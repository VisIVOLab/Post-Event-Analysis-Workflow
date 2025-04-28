# CWL Photogrammetry Pipeline

## Overview
This part of the project uses the Common Workflow Language (CWL) to orchestrate the photogrammetry pipeline, executing each task locally in a reproducible environment. To handle task progression and manage dependencies between steps, the current implementation uses the generation of dummy files as intermediate signals.

| ⚠️ Note: This is work in progress.

It's recommended to convert the relative paths of cwl scripts into absolute paths

## Requirements
- cwltool
- Python

## Project Structure
```
cwl/
├── config/                     # Metashape definitions
├── step/                       # Python scripts
├── inputs/
│   └── params.json             # input photogrammetric process params
├── cwl-photogrammetry/
│   ├── init_config.cwl
│   ├── new_project.cwl
│   ├── import_photos.cwl
│   ├── match_align_photos.cwl
│   ├── build_depth_maps.cwl
│   ├── build_point_cloud.cwl
│   ├── build_model.cwl
│   ├── build_tiled.cwl
│   └── photogrammetry_workflow_from_depth.cwl
├── job.yaml                    # input refere to inputs/params.json
└── init_out.json               # support file generated during the process
```

### Configuration

[`params.json`](inputs/params.json) defines the photogrammetry configuration setting
```json
"settings": {
  ...
}
"matchPhotos": {
  ...
},
"alignCameras": {
  ...
},
"buildDepthMaps": {
  ...
},
"buildPointCloud": {
  ...
},
"buildModel": {
  ...
},
"buildTiledModel": {
  ...
}
```

## Run the CWL
1. Set [`params.json`](inputs/params.json)

2. Run CWL 
    ```bash
    cwltool cwl/photogrammetry_workflow_from_depth.cwl job.yaml
    ```