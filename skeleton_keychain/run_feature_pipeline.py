import os
import argschema as ags
import numpy as np
import time
from skeleton_keys.database_queries import query_for_swc_file
import shutil
from skeleton_keychain.SlurmDAG import create_job_file, submit_job_return_id


class IO_Schema(ags.ArgSchema):
    input_specimen_id_txt = ags.fields.InputFile(description='input txt with specimen id list')
    output_dir = ags.fields.OutputDir(description="Where to dump all output files")
    calculate_features = ags.fields.Boolean(default=True, description="When false will only generate swc files")
    orientation_independent_features = ags.fields.Bool(allow_none=True,
                                                       description=" set to True when you want to run features without generating a soma depth or histogram file (e.g. subcotical)")
    shrinkage_correction = ags.fields.Boolean(description="If true, correct for shrinkage")
    slice_angle_tilt_correction = ags.fields.Boolean(description="If true, correct for slice angle tilt")

    species = ags.fields.Str(description="mouse or human", allow_none=True, default=None)
    aligned_swc_dir = ags.fields.InputDir(description="Directory to use for feature calc", default=None,
                                          allow_none=True)
    upright_swc_dir = ags.fields.InputDir(description="Directory to use for feature calc", default=None,
                                          allow_none=True)
    raw_orientation_swc_dir = ags.fields.InputDir(description="Raw Orientation Directory", default=None,
                                                  allow_none=True)
    polygon_json_dir = ags.fields.InputDir(description="Layers polygon directory", default=None, allow_none=True)

    analyze_axon = ags.fields.Boolean(default=False, allow_none=True)
    analyze_basal_dendrite = ags.fields.Boolean(default=True, allow_none=True)
    analyze_apical_dendrite = ags.fields.Boolean(default=False, allow_none=True)
    layer_depths_file = ags.fields.InputFile(default=None, allow_none=True,
                                             description="Json with average cortical layer depths")
    surface_paths_file = ags.fields.InputFile(
        default=None,
        allow_none=True,
        description="Surface paths (streamlines) HDF5 file for slice angle calculation (e.g. surface_paths_10_v3.h5)",
    )
    closest_surface_voxel_file = ags.fields.InputFile(
        default=None,
        allow_none=True,
        description="Closest surface voxel reference HDF5 file for slice angle calculation (e.g. closest_surface_voxel_lookup.h5)",
    )

    slurm_virtual_env = ags.fields.Str(description="Name of the virtual environment to run features on")
    axon_depth_profile_loadings_file = ags.fields.InputFile(
        default=None,
        allow_none=True,
        description="CSV with pre-existing axon depth profile loadings",
    )
    basal_dendrite_depth_profile_loadings_file = ags.fields.InputFile(
        default=None,
        allow_none=True,
        description="CSV with pre-existing basal dendrite depth profile loadings",
    )
    apical_dendrite_depth_profile_loadings_file = ags.fields.InputFile(
        default=None,
        allow_none=True,
        description="CSV with pre-existing apical dendrite depth profile loadings",
    )
    save_axon_depth_profile_loadings_file = ags.fields.OutputFile(
        default=None,
        allow_none=True,
        description="Output file to save axon depth profile loadings",
    )
    save_basal_dendrite_depth_profile_loadings_file = ags.fields.OutputFile(
        default=None,
        allow_none=True,
        description="Output file to save basal dendrite depth profile loadings",
    )
    save_apical_dendrite_depth_profile_loadings_file = ags.fields.OutputFile(
        default=None,
        allow_none=True,
        description="Output file to save apical dendrite depth profile loadings",
    )


def main(input_specimen_id_txt,
         output_dir,
         calculate_features,
         orientation_independent_features,
         species,
         aligned_swc_dir,
         upright_swc_dir,
         slurm_virtual_env,
         layer_depths_file,
         raw_orientation_swc_dir,
         polygon_json_dir,
         shrinkage_correction,
         slice_angle_tilt_correction,
         analyze_apical_dendrite,
         analyze_basal_dendrite,
         analyze_axon,
         axon_depth_profile_loadings_file,
         basal_dendrite_depth_profile_loadings_file,
         apical_dendrite_depth_profile_loadings_file,
         surface_paths_file,
         closest_surface_voxel_file,
         save_axon_depth_profile_loadings_file,
         save_basal_dendrite_depth_profile_loadings_file,
         save_apical_dendrite_depth_profile_loadings_file,
         **kwargs):

    # validation
    if calculate_features is not None:
        if orientation_independent_features is None:
            raise ValueError("--calculate_features was set to True but you did not specify "
                             "--orientation_independent_features to True or False ")



    # core_files_to_delete = [f for f in os.listdir(".") if "core." in f and ".py" not in f]
    # for fi in core_files_to_delete:
    #     print("Deleting: {}".format(fi))
    #     os.remove(fi)

    execution_dir = os.path.abspath(".")
    cd_command = "cd {}".format(execution_dir)

    if species == "mouse":
        layer_list = ["Layer1", "Layer2/3", "Layer4", "Layer5", "Layer6a", "Layer6b"]
    elif species == "human":
        layer_list = ["Layer1", "Layer2", "Layer3", "Layer4", "Layer5", "Layer6"]
    layer_list = '"[' + "".join([f"'{lyr}', " for lyr in layer_list]) + ']"'

    specimen_ids = np.loadtxt(input_specimen_id_txt, dtype=str)
    # specimen_ids = pd.read_csv(input_specimen_id_txt)['specimen_id'].values
    print("Number of Specimens to generate files for: {}".format(len(specimen_ids)))
    time.sleep(2)

    job_dir = os.path.join(output_dir, "JobFiles")
    if not os.path.exists(job_dir):
        os.mkdir(job_dir)

    swc_file_gen_job_ids = []
    dag_id = 0
    if (aligned_swc_dir is None) and (upright_swc_dir is None) and (not orientation_independent_features):

        # We need to generate the files and expect to be able to do so through database queries
        upright_swc_dir = os.path.join(output_dir, "SWC_Upright")
        feature_swc_dir = upright_swc_dir

        aligned_swc_dir = os.path.join(output_dir, "SWC_LayerAligned")
        qc_image_dir = os.path.join(output_dir, "SWC_QC_Images")
        check_make_list = [upright_swc_dir, aligned_swc_dir, qc_image_dir]
        for dd in check_make_list:
            if not os.path.exists(dd):
                os.mkdir(dd)


        for sp_id in specimen_ids:
            dag_id += 1

            la_file = os.path.abspath(os.path.join(aligned_swc_dir, "{}.swc".format(sp_id)))
            ur_file = os.path.abspath(os.path.join(upright_swc_dir, "{}.swc".format(sp_id)))
            log_file = os.path.abspath(os.path.join(job_dir, "{}.out".format(sp_id)))
            job_file = os.path.abspath(os.path.join(job_dir, "{}.sh".format(sp_id)))
            qc_image_file = os.path.abspath(os.path.join(qc_image_dir, "{}.png".format(sp_id)))

            raw_swc_file, polygon_json = None, None
            if raw_orientation_swc_dir is not None:
                # In this scenario we have raw swc files that we will need to upright/layeralign based on drawings
                raw_swc_file = os.path.abspath(os.path.join(raw_orientation_swc_dir, "{}.swc".format(sp_id)))

            if polygon_json_dir is not None:
                polygon_json = os.path.abspath(os.path.join(polygon_json_dir, "{}.json".format(sp_id)))

            # resource request from slurm
            slurm_resource_kwargs = {
                "--job-name": f"seg-{sp_id}",
                "--mail-type": "NONE",
                "--nodes": "1",
                "--kill-on-invalid-dep": "yes",
                "--cpus-per-task": "2",
                "--mem": "10gb",
                "--time": "96:00:00",
                "--partition": "celltypes",
                "--output": log_file
            }

            # what you want to run on slurm
            upright_command_kwargs = {
                "specimen_id": sp_id,
                "output_file": ur_file,
                "closest_surface_voxel_file": closest_surface_voxel_file,
                "surface_paths_file": surface_paths_file,
                "swc_path": raw_swc_file,
                "surface_and_layers_file": polygon_json,
                "correct_for_shrinkage": shrinkage_correction,
                "correct_for_slice_angle": slice_angle_tilt_correction,
            }
            upright_command_kwargs = {k: v for k, v in upright_command_kwargs.items() if v is not None}
            upright_command_kwargs = " ".join(["--{} {}".format(k, v) for k, v in upright_command_kwargs.items()])

            layer_align_command_kwargs = {
                "specimen_id": sp_id,
                "output_file": la_file,
                "layer_depths_file": layer_depths_file,
                "closest_surface_voxel_file": closest_surface_voxel_file,
                "surface_paths_file": surface_paths_file,
                "swc_path": raw_swc_file,
                "surface_and_layers_file": polygon_json,
                "correct_for_shrinkage": shrinkage_correction,
                "correct_for_slice_angle": slice_angle_tilt_correction,
                "layer_list": layer_list,
            }
            layer_align_command_kwargs = {k: v for k, v in layer_align_command_kwargs.items() if v is not None}
            layer_align_command_kwargs = " ".join(
                ["--{} {}".format(k, v) for k, v in layer_align_command_kwargs.items()])

            qc_image_command_kwargs = {
                "ur_swc": ur_file,
                "la_swc": la_file,
                "qc_image_file": qc_image_file,
                "layer_depths_file": layer_depths_file
            }
            qc_image_command_kwargs = " ".join(
                ["--{} {}".format(k, v) for k, v in qc_image_command_kwargs.items() if v is not None])

            slurm_commands = [
                "source ~/.bashrc",
                f"conda activate {slurm_virtual_env}",
                cd_command,
                "skelekeys-layer-aligned-swc {}".format(layer_align_command_kwargs),
                "skelekeys-upright-corrected-swc {}".format(upright_command_kwargs),
                "qc_swc_image {}".format(qc_image_command_kwargs)
            ]

            # bringing it all together
            file_gen_dag_node = {
                "id": dag_id,  # this id is not the same as slurm job id.
                "parent_id": -1,  # this job has no upstream dependency
                "name": "{}-file-gen".format(sp_id),
                "job_file": job_file,
                "slurm_kwargs": slurm_resource_kwargs,
                "slurm_commands": slurm_commands,
            }

            create_job_file(file_gen_dag_node)
            slurm_job_id = submit_job_return_id(job_file=job_file, parent_job_id=None, start_condition=None)

            swc_file_gen_job_ids.append(slurm_job_id)


    elif orientation_independent_features:
        if raw_orientation_swc_dir is None:
            # we need to get the raw swc files and put them somewhere
            swc_dst_dir = os.path.join(output_dir, "RawFilesFromLims")
            if not os.path.exists(swc_dst_dir):
                os.mkdir(swc_dst_dir)

            for sp_id in specimen_ids:
                swc_src_pth = query_for_swc_file(sp_id)
                swc_dst_pth = os.path.join(swc_dst_dir, f"{sp_id}.swc")
                shutil.copy(swc_src_pth, swc_dst_pth)

            feature_swc_dir = swc_dst_dir
        else:
            feature_swc_dir = raw_orientation_swc_dir


    else:
        # validate the provided swc directories
        if aligned_swc_dir is not None:
            assert os.path.exists(aligned_swc_dir)
            assert len([f for f in os.listdir(aligned_swc_dir) if f.endswith(".swc")]) != 0

        if upright_swc_dir is not None:
            assert os.path.exists(upright_swc_dir)
            assert len([f for f in os.listdir(upright_swc_dir) if f.endswith(".swc")]) != 0

    if calculate_features:

        histogram_ofile = None
        soma_depth_ofile = None
        histogram_job_id = None
        if not orientation_independent_features:
            # Generate Auxilary Files Needed For Feature Calc
            histo_job_file = os.path.abspath(os.path.join(job_dir, "histogram_job.sh"))
            histo_log_file = os.path.abspath(os.path.join(job_dir, "histogram_job.log"))

            histogram_ofile = os.path.join(output_dir, "AlignedHistogram.csv")
            soma_depth_ofile = os.path.join(output_dir, "AlignedSomaDepths.csv")
            aux_file_input_args = {"specimen_id_file": input_specimen_id_txt,
                                   "swc_dir": aligned_swc_dir,
                                   "output_hist_file": histogram_ofile,
                                   "output_soma_file": soma_depth_ofile,
                                   "layer_depths_file": layer_depths_file}
            profile_cmd = "skelekeys-profiles-from-swcs " + " ".join(
                ["--{} {}".format(k, v) for k, v in aux_file_input_args.items()])

            # resource request from slurm
            histo_slurm_resource_kwargs = {
                "--job-name": f"histogram-gen",
                "--mail-type": "NONE",
                "--nodes": "1",
                "--kill-on-invalid-dep": "no",
                "--cpus-per-task": "2",
                "--mem": "10gb",
                "--time": "60",
                "--partition": "celltypes",
                "--output": histo_log_file
            }
            histo_slurm_commands = [
                "source ~/.bashrc",
                f"conda activate {slurm_virtual_env}",
                cd_command,
                profile_cmd
            ]

            histo_file_gen_dag_node = {
                "id": dag_id + 1,  # this id is not the same as slurm job id.
                "parent_id": -1,  # this job has no upstream dependency
                "name": "histo-file-gen",
                "job_file": histo_job_file,
                "slurm_kwargs": histo_slurm_resource_kwargs,
                "slurm_commands": histo_slurm_commands,
            }

            create_job_file(histo_file_gen_dag_node)
            if swc_file_gen_job_ids != []:
                histogram_job_id = submit_job_return_id(job_file=histo_job_file, parent_job_id=swc_file_gen_job_ids,
                                                        start_condition="afterany")
            else:
                histogram_job_id = submit_job_return_id(job_file=histo_job_file, parent_job_id=None,
                                                        start_condition=None)

            # adding histogram job id to this will let our feature calculation job to wait until its finished
            swc_file_gen_job_ids.append(histogram_job_id)

        # Calculate Features
        feature_ofile = os.path.join(output_dir, "RawFeatureLong.csv")
        wide_norm_ofile = os.path.join(output_dir, "NormFeatureWide.csv")
        wide_unnorm_ofile = os.path.join(output_dir, "RawFeatureWide.csv")
        feat_calc_input_cfigs = {"specimen_id_file": input_specimen_id_txt,
                                 "swc_dir": feature_swc_dir,
                                 "aligned_soma_file": soma_depth_ofile,
                                 "aligned_depth_profile_file": histogram_ofile,
                                 "analyze_axon": analyze_axon,
                                 "analyze_basal_dendrite": analyze_basal_dendrite,
                                 "analyze_apical_dendrite": analyze_apical_dendrite,
                                 "axon_depth_profile_loadings_file": axon_depth_profile_loadings_file,
                                 "basal_dendrite_depth_profile_loadings_file": basal_dendrite_depth_profile_loadings_file,
                                 "apical_dendrite_depth_profile_loadings_file": apical_dendrite_depth_profile_loadings_file,
                                 "save_axon_depth_profile_loadings_file": save_axon_depth_profile_loadings_file,
                                 "save_basal_dendrite_depth_profile_loadings_file": save_basal_dendrite_depth_profile_loadings_file,
                                 "save_apical_dendrite_depth_profile_loadings_file": save_apical_dendrite_depth_profile_loadings_file,
                                 "output_file": feature_ofile
                                 }
        feat_cmd = "skelekeys-morph-features " + " ".join(
            ["--{} {}".format(k, v) for k, v in feat_calc_input_cfigs.items() if v is not None])
        feat_post_proc_cmd = 'skelekeys-postprocess-features --input_files "' + f"['{feature_ofile}'" + f']" --wide_normalized_output_file {wide_norm_ofile} --wide_unnormalized_output_file {wide_unnorm_ofile}'

        feature_job_file = os.path.abspath(os.path.join(job_dir, "Feature_Calculation_Job.sh"))
        feature_job_log_file = os.path.abspath(os.path.join(job_dir, "Feature_Calculation_Job.out"))

        feature_slurm_resource_kwargs = {
            "--job-name": f"features-calc",
            "--mail-type": "NONE",
            "--nodes": "1",
            "--kill-on-invalid-dep": "no",
            "--cpus-per-task": "70",
            "--mem": "120gb",
            "--time": "124:00:00",
            "--partition": "celltypes",
            "--output": feature_job_log_file
        }
        feature_slurm_commands = [
            "source ~/.bashrc",
            f"conda activate {slurm_virtual_env}",
            cd_command,
            feat_cmd,
            feat_post_proc_cmd
        ]
        feature_gen_dag_node = {
            "id": dag_id + 2,  # this id is not the same as slurm job id.
            "parent_id": -1,  # this job has no upstream dependency
            "name": "histo-file-gen",
            "job_file": feature_job_file,
            "slurm_kwargs": feature_slurm_resource_kwargs,
            "slurm_commands": feature_slurm_commands,
        }

        create_job_file(feature_gen_dag_node)

        if swc_file_gen_job_ids != []:
            submit_job_return_id(job_file=feature_job_file, parent_job_id=swc_file_gen_job_ids,
                                 start_condition="afterany")
        else:
            submit_job_return_id(job_file=feature_job_file, parent_job_id=None, start_condition=None)


if __name__ == "__main__":
    module = ags.ArgSchemaParser(schema_type=IO_Schema)
    main(**module.args)


def console_script():
    module = ags.ArgSchemaParser(schema_type=IO_Schema)
    main(**module.args)
