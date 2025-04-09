import json
import os
import sys
from pathlib import Path

dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
sys.path.append(str(project_root_folder))

def build_model():
    #import Metashape
    import logging
    from config.surface_type import surface_types
    from config.interpolation import interpolations
    from config.face_count import face_counts
    from config.data_source import data_sources

    """Build 3D model process"""
    with open(sys.argv[1], 'r') as f:
        init_out = json.load(f)

    project_path = init_out.get('project_path')
    output_folder = init_out.get('output_path')

    config_path = sys.argv[1]
    with open(config_path) as f:
        cfg = json.load(f)

    task_config = cfg.get("buildModel", {})
    surface_type_str = task_config.get('surface_type', "Metashape.SurfaceType.Arbitrary")
    #surface_type = surface_types.get(surface_type_str, Metashape.SurfaceType.Arbitrary)
    interpolation_str = task_config.get('interpolation', "Metashape.Interpolation.EnabledInterpolation")
    #interpolation = interpolations.get(interpolation_str, Metashape.Interpolation.EnabledInterpolation)
    face_count_str = task_config.get('face_count', "Metashape.FaceCount.MediumFaceCount")
    #face_count = face_counts.get(face_count_str, Metashape.FaceCount.MediumFaceCount)
    source_data_str = task_config.get('source_data', "Metashape.DataSource.DepthMapsData")
    #source_data = data_sources.get(source_data_str, Metashape.DataSource.DepthMapsData)
    vertex_colors = task_config.get('vertex_colors', True)
    vertex_confidence = task_config.get('vertex_confidence', True)
    keep_depth = task_config.get('keep_depth', True)
    split_in_blocks = task_config.get('split_in_blocks', False)
    blocks_size = task_config.get('blocks_size', 250)
    build_texture = task_config.get('build_texture', True)
    subdivide_task = task_config.get('subdivide_task', True)

    #doc = Metashape.Document()
    #doc.open(path=project_path, read_only=False)
    #chunk = doc.chunks[0]
    #Metashape.app.cpu_enable = init_out.get('cpu_enable')
    #Metashape.app.gpu_mask = init_out.get('gpu_mask')

    """ chunk.buildModel(surface_type=surface_type, 
                     interpolation=interpolation, 
                     face_count=face_count, 
                     source_data=source_data,
                     vertex_colors=vertex_colors,
                     vertex_confidence=vertex_confidence,
                     keep_depth = keep_depth,
                     split_in_blocks = split_in_blocks,
                     blocks_size = blocks_size,
                     build_texture= build_texture,
                     subdivide_task = subdivide_task) """
    #chunk.buildUV(mapping_mode= Metashape.MappingMode.GenericMapping, page_count=1, texture_size=8192)
    #chunk.buildTexture(blending_mode= Metashape.BlendingMode.MosaicBlending, texture_size= 8192, fill_holes= True, ghosting_filter= True)
    #chunk.exportModel(os.path.join(output_folder, 'model.obj'))
    #chunk.exportTexture(path=os.path.join(output_folder, 'texture.jpg'), texture_type= Metashape.Model.TextureType.DiffuseMap, save_alpha= False,  raster_transform= Metashape.RasterTransformType.RasterTransformNone)
    logging.info(f"Export 3D model.")
    sys.exit(0)

if __name__ == "__main__":
    build_model()
