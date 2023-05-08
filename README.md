# skeleton-keychain
This package links together commands from the skeleton-keys package to run typical steps of feature calculation/file generation pipelines on a slurm
hpc system. 

Installation instructions
=========================
See https://github.com/AllenInstitute/skeleton_keys
to install skeleton-keys.    
  
Then:  
`pip install morph_utils`  


## Entry Points
After installation the `run_features`  console script will be available to run from the command line of your environment. 
This flexible script can be used to generate features, uprighted & layer-aligned swc files, histograms and soma depth 
csvs from a variety of input sources as detailed below.   

A quick note on file nomenclature. It is expected that when passing in files from local sources (i.e. not getting swc/layers from lims) that they
are named with the convention of specimenID.extension.  So if you are 
trying to layer-align a local file named: `1234_my_processing.swc` where `1234` represents the cell's specimen ID as seen in
LIMS databse, the processes will fail because it is expecting just `1234.swc`  



#### 1. Manual Traced Cortical Data 
A list of cortical specimen IDs that have Pia/WM/Soma/Layers in lims  
    (NOTE: if you want to only generate layer-aligned and upright swc files, the `run_features` script should be run with `--calculate_features False`)
    
    
#### 2. Manual Traced Subcortical
A list of subcortical specimen IDs that do not require Pia/WM/Soma/Layers in lims, only an swc file   
  (NOTE: in this config, you will want to run `run_features` with `--orientation_independent_features True`)  
  (NOTE: if you want to only download swc files, the `run_features` script should be run with `--calculate_features False`)
  
  
#### 3. Autotrace  Cortical/Subcortical
A directory of cortical or subcortical autotrace data. This expects the data are micron image oriented, as such 
you should use the `--raw_orientation_swc_dir` argument. If you just want to layer align
and upright autotraced cortical data, that can be done with this configuration
and `--calculate_features False`


  
#### 4. Precomputed Layer-Aligned/Upright Files  
If you have a directory of upright and layer aligned swc files those can be passed 
in with `--aligned_swc_dir` and `--upright_swc_dir`
  
#### 5. Electron Microscopy Data 
This data typically comes with a "raw" oriented swc file and a .json file which contains Pia/WM/Soma/Layers
data. To use this configuration you will pass in the raw swc files with `--raw_orientation_swc_dir` and 
the json directory with `polygon_json_dir`. 
  

## Explanation of some keyword arguments

layer_depths_file - json with the average depths for cortical layers. These are the reference depths that we layer-align the data to   
 
surface_paths_file - Surface paths (streamlines) HDF5 file for slice angle calculation (e.g. surface_paths_10_v3.h5)  
  
closest_surface_voxel_file - Closest surface voxel reference HDF5 file for slice angle calculation (e.g. closest_surface_voxel_lookup.h5)  
  
slurm_virtual_env - name of the conda virtual environment that has this package and skeleton-keys installed  
 
axon/apical/basal_depth_profile_loadings_file - a file that contains the weights from PCA. Example use case, running features 
on fMOST data and you want your histogram PCA features to capture variation from patch-seq data 


## Example Usage
Entry Point 1 example. This will generate layer-aligned and upright swc files for
a list of mouse cortical specimens. It will then generate histogram, soma depth and various
feature csv files in the output directory.

```
run_features --input_specimen_id_txt path/to/cortical_mouse/specimens_ids.txt 
--output_dir path/to/results/directory
--calculate_features True
--orientation_independent_features False
--shrinkage_correction True
--slice_angle_tilt_correction True
--species mouse
--analyze_basal_dendrite True
--analyze_apical_dendrite True
--layer_depths_file path/to/avg_layer_depths.json
--surface_paths_file path/to/surface_paths_10_v3.h5
--closest_surface_voxel_file /path/to/closest_surface_voxel_lookup.h5
--slurm_virtual_env my_skkeys_environment
```
Statement of Support
====================
This code is an important part of the internal Allen Institute code base and we are actively using and maintaining it. Issues are encouraged, but because this tool is so central to our mission pull requests might not be accepted if they conflict with our existing plans.