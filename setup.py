from pathlib import Path
from setuptools import setup

package_dir = Path(__file__).parent.joinpath("src", "nqdc")
data_files = [
    str(f.relative_to(package_dir))
    for f in package_dir.joinpath("_data").glob("**/*")
]
version = package_dir.joinpath("_data", "VERSION").read_text("utf-8").strip()
setup(package_data={"nqdc": data_files}, version=version)
