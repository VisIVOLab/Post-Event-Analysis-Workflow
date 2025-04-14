import json
import os
import sys

def match_and_align():
    #import Metashape
    import logging
    
    """
    Image matching and alignment
    """

    with open(sys.argv[2], 'r') as f:
        init_out = json.load(f)

    project_path = init_out.get('project_path')
    #Metashape.app.cpu_enable = init_out.get('cpu_enable')
    #Metashape.app.gpu_mask = init_out.get('gpu_mask')

    config_path = sys.argv[1]
    with open(config_path, 'r') as f:
        cfg = json.load(f)

    task_config = cfg.get("matchPhotos", {})
    downscale = task_config.get('downscale', 2)
    downscale_3d = task_config.get('downscale_3d', 4)
    keypoint_limit = task_config.get('keypoint_limit', 40000)
    tiepoint_limit = task_config.get('tiepoint_limit', 10000)
    generic_preselection = task_config.get('generic_preselection', True)
    reference_preselection = task_config.get('reference_preselection', False)
    filter_stationary_points = task_config.get('filter_stationary_points', True)
    keep_keypoints = task_config.get('keep_keypoints', True)
    guided_matching = task_config.get('guided_matching', False)
    subdivide_task = task_config.get('subdivide_task', True)

    #doc = Metashape.Document()
    #doc.open(path=project_path, read_only=False)
    #chunk = doc.chunks[0]

    """ chunk.matchPhotos(downscale=downscale, 
                      downscale_3d=downscale_3d, 
                      generic_preselection= generic_preselection, 
                      keypoint_limit=keypoint_limit, 
                      tiepoint_limit=tiepoint_limit,
                      reference_preselection = reference_preselection,
                      filter_stationary_points = filter_stationary_points,
                      keep_keypoints = keep_keypoints,
                      guided_matching = guided_matching,
                      subdivide_task = subdivide_task) """
    
    task_config = cfg.get("alignCameras", {})
    adaptive_fitting = task_config.get('adaptive_fitting', False)
    reset_alignment = task_config.get('reset_alignment', True)
    subdivide_task = task_config.get('subdivide_task', True)

    """ chunk.alignCameras(adaptive_fitting = adaptive_fitting,
                       reset_alignment = reset_alignment,
                       subdivide_task= subdivide_task) """
    
    #doc.save(version="match_and_align")
    print(f"Matched & Align photos.")
    with open("match_and_align.done", "w") as f:
        f.write("match and align done")
    sys.exit(0)

if __name__ == "__main__":
    match_and_align()