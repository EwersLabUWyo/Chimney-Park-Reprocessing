# Chimney Park Re-Processing Project

Re-processing data from the Chimney Park Ameriflux site from 2008 - 2022 (and after)
Sandbox to explore ways of re-processing chimney park data. This contains data from weeks with both "good" (few problems) and "bad" (many problems) data.

# Important logistical information

Due to the enourmous size of eddy covariance files, let's not upload any data files to the github. Instead, please keep all data files on Alcova. If Alcova is slow to work with, consider working with the data locally, then sharing it by uploading it to the Alcova library and updating your code to point there when you add a commit to this repository. 

Currently, the .gitignore is set to ignore files labelled with "10Hz*.dat", "30Min*.dat" and ".hobo" extensions. If the data files you're working with have different naming conventions than these, please add those to the .gitignore. For example, if you generate any .csv files, add those to the .gitignore as `<path>/<to>/<files>/*.csv`
