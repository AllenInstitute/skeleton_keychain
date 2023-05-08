from neuron_morphology.swc_io import *
import matplotlib.pyplot as plt
from morph_utils.visuals import basic_morph_plot
import json
import argschema as ags
import os

class IO_Schema(ags.ArgSchema):
    ur_swc = ags.fields.InputFile(description='input txt with specimen id list')
    la_swc = ags.fields.InputFile(description='input txt with specimen id list')
    qc_image_file = ags.fields.OutputFile(description='input txt with specimen id list')
    layer_depths_file = ags.fields.InputFile(description="layer depths file")

def main(ur_swc,la_swc,qc_image_file,layer_depths_file, **kwargs):

    filename = os.path.basename(ur_swc)
    with open(layer_depths_file,"r") as f:
        layer_depths = json.load(f)
        layer_depths['1']=0


    ur_morph = morphology_from_swc(ur_swc)
    la_morph = morphology_from_swc(la_swc)
    # la_morph_soma = la_morph.get_soma()
    # aff_center = [1,0,0, 0,1,0, 0,0,1, -la_morph_soma['x'],0,-la_morph_soma['z']]
    # la_morph = aff.from_list(aff_center).transform_morphology(la_morph)
    # morphology_to_swc(la_morph,la_swc)
    print("Layer Aligned Soma")
    print(la_morph.get_soma())

    fig, axe = plt.subplots(1, 2)
    basic_morph_plot(morph=ur_morph,
                     ax=axe[0],
                     title='upright',
                     line_w=1,
                     side=False,
                     scatter=False)
    basic_morph_plot(morph=la_morph,
                     ax=axe[1],
                     title='layer aligned',
                     line_w=1,
                     side=False,
                     scatter=False)

    for a in axe:
        a.set_aspect('equal')

    for v in layer_depths.values():
        axe[1].axhline(-v, c='lightgrey', linestyle='--')

    fig.suptitle("sp: {}".format(filename), ha='center', y=1.15)
    fig.set_size_inches(9, 4)
    fig.savefig(qc_image_file, dpi=300, bbox_inches='tight')
    plt.clf()

    # core_files_to_delete = [f for f in os.listdir(".") if "core." in f and ".py" not in f]
    # for fi in core_files_to_delete:
    #     print("Deleting: {}".format(fi))
    #     os.remove(fi)


if __name__ == "__main__":
    module = ags.ArgSchemaParser(schema_type=IO_Schema)
    main(**module.args)
