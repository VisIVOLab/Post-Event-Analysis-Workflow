import json
import os
import Metashape

def init_config():

    script_dir = os.path.dirname(os.path.abspath(__file__))
    params_path = os.path.join(script_dir, '..', 'inputs_photogrammetry', 'params.json')

    with open(params_path, 'r') as f:
        init_out = json.load(f)

    project_out = "/photogrammetry/project"
    project_path = os.path.join(project_out, "project.psx")
    image_path = "/photogrammetry/images"

    project_sett = init_out.get('settings', {})
    cpu_enable = project_sett.get('cpu_enable', False)
    gpu_mask = project_sett.get('gpu_mask')
    if not isinstance(gpu_mask, int):
        # GPU info
        gpus = Metashape.app.enumGPUDevices()
        num_gpus = len(gpus)
        gpu_mask = 2**num_gpus - 1 if num_gpus > 0 else 0

    # Write outputs
    with open("init_set.json", "w") as f:
        json.dump({
            "project_path": project_path,
            "project_output": project_out,
            "image_path": image_path,
            "cpu_enable": cpu_enable,
            "gpu_mask": gpu_mask
        }, f)

if __name__ == "__main__":
    init_config()
