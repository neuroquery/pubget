from pathlib import Path
from setuptools import setup

package_dir = Path(__file__).parent.joinpath("src", "nqdc")
data_files = [
    str(f.relative_to(package_dir))
    for f in package_dir.joinpath("data").glob("**/*")
]
setup(package_data={"nqdc": data_files})
