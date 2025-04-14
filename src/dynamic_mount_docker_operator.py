from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount



class DynamicMountDockerOperator(DockerOperator):
    """
    Custom DockerOperator that allows dynamic mounting of input and output paths.
    """


    # make 'source_path' a templated field
    template_fields = DockerOperator.template_fields + ("input_path","output_path",)

    def __init__(
        self,
        input_path: str,  
        target_input_path: str,   
        src_path: str,
        target_src_path: str,
        output_path: str,
        target_output_path: str,

        **kwargs,
    ):
        super().__init__(**kwargs)
        self.input_path = input_path
        self.target_input_path = target_input_path

        self.src_path = src_path
        self.target_src_path = target_src_path

        self.output_path = output_path
        self.target_output_path = target_output_path

    def execute(self, context):
        self.mounts = [
            Mount(source=self.input_path, target=self.target_input_path , type="bind", read_only=True),
            Mount(source=self.src_path, target=self.target_src_path, type="bind", read_only=True),
            Mount(source=self.output_path, target=self.target_output_path, type="bind", read_only=False),
        ]
        return super().execute(context)