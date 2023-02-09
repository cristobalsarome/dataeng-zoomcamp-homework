# this code does not work in a jupyter notebook
from prefect.filesystems import GitHub
block = GitHub(
    repository="https://github.com/cristobalsarome/dataeng-zoomcamp-homework/"
)
 # specify a subfolder of repo
block.get_directory("week-2") 
block.save("gitrepo1")