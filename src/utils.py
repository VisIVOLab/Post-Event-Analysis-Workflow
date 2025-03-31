import os
from pathlib import Path
import warnings


dag_folder = os.path.dirname(os.path.abspath(__file__))
project_root_folder = Path(dag_folder).parent
data_folder = project_root_folder / 'data'


# A simple persistent boolean flag stored as a file in the project data folder
class Flag():
    def __init__(self, name, value):
        print(f" type of name {type(name)}")
        assert isinstance(name, str), "name must be a string"
        assert isinstance(value, bool), "value must be a boolean"
        self.name = name
        self.value = value

        self.filename = f"flag-{self.name}"
        self.filepath = data_folder / self.filename

        # If the file exists, read its content and validate it
        if self.filepath.exists():
            with open(self.filepath, "r") as f:
                content = f.read().strip()
                if content == "True":
                    file_value = True
                elif content == "False":
                    file_value = False
                else:
                    raise ValueError(f"Invalid content in file {self.filepath}: '{content}'")

            if file_value != self.value:
                warnings.warn(
                    f"Flag mismatch: provided value {self.value} differs from file value {file_value}. Using file value."
                )
            self.value = file_value
        else:
            # File doesn't exist: write the provided value to the file
            with open(self.filepath, "w") as f:
                f.write("True" if self.value else "False")
     
    def get(self):
        if self.filepath.exists():
            with open(self.filepath, "r") as f:
                content = f.read().strip()
                if content == "True":
                    return True
                elif content == "False":
                    return False
                else:
                    raise ValueError(f"Invalid content in file {self.filepath}: '{content}'")
        else:
            raise FileNotFoundError(f"Flag file {self.filepath} not found.")

    def true(self):
        self.value = True
        with open(self.filepath, "w") as f:
            f.write("True")

    def false(self):
        self.value = False
        with open(self.filepath, "w") as f:
            f.write("False")


    def set_filepath(self, new_path):
        """Set a new filepath for the flag and update the file location."""
        self.filepath = Path(new_path)
        with open(self.filepath, "w") as f:
            f.write("True" if self.value else "False")