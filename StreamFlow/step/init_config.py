import json
import os
import sys
#import Metashape

def main():
    config_path = sys.argv[1]
    
    with open(config_path) as f:
        cfg = json.load(f)

    task_config = cfg.get("settings", {})
    project_path = task_config.get('project_path', '.')
    project_path = os.path.join(project_path, "project.psx")
    
    image_path = task_config.get('image_path', None)
    if image_path is None:
        raise ValueError("Error: no image_path folder selected!")

    # GPU info
    #gpus = Metashape.app.enumGPUDevices()
    #num_gpus = len(gpus)
    #gpu_mask = 2**num_gpus - 1 if num_gpus > 0 else 0

    # Write outputs
    with open("init_out.json", "w") as f:
        json.dump({
            "project_path": project_path,
            "image_path": image_path
            #"num_gpus": num_gpus,
            #"gpu_mask": gpu_mask
        }, f)

if __name__ == "__main__":
    main()
