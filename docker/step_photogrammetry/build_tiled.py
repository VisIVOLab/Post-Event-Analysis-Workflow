import json
import os
import sys
from pathlib import Path

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))

def build_tiled():
    import Metashape
    from config.data_source import data_sources

    """
    Build Tiled model process
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    params_path = os.path.join(script_dir, '..', 'init_set.json')

    with open(params_path, 'r') as f:
        init_out = json.load(f)

    project_path = init_out.get('project_path')
    output_folder = init_out.get('project_output')

    config_path = os.path.join(script_dir, '..', 'inputs_photogrammetry', 'params.json')
    with open(config_path, 'r') as f:
        cfg = json.load(f)

    task_config = cfg.get("buildTiledModel", {})
    pixel_size = task_config.get('pixel_size', 0)
    tile_size = task_config.get('tile_size', 256)
    source_data_str = task_config.get('source_data', "Metashape.DataSource.DepthMapsData")
    source_data = data_sources.get(source_data_str, Metashape.DataSource.DepthMapsData)
    face_count = task_config.get('face_count', 20000)
    ghosting_filter = task_config.get('ghosting_filter', False)
    transfer_texture = task_config.get('transfer_texture', False)
    keep_depth = task_config.get('keep_depth', True)
    subdivide_task = task_config.get('subdivide_task', True)

    doc = Metashape.Document()
    doc.open(path=project_path, read_only=True)
    chunk = doc.chunks[0]
    Metashape.app.cpu_enable = init_out.get('cpu_enable')
    Metashape.app.gpu_mask = init_out.get('gpu_mask')

    chunk.buildTiledModel(pixel_size = pixel_size,
                          tile_size = tile_size,
                          source_data=source_data,
                          face_count = face_count,
                          ghosting_filter = ghosting_filter,
                          transfer_texture = transfer_texture,
                          keep_depth = keep_depth,
                          subdivide_task = subdivide_task)
    chunk.exportTiledModel(path=os.path.join(output_folder, 'tile.zip'), format=Metashape.TiledModelFormat.TiledModelFormatZIP)
    print(f" Export tiled model.")
    sys.exit(0)

if __name__ == "__main__":
    build_tiled()