import glob
import json
import os
N_FILES_MAX_PER_SAMPLE = config["n_files_max_per_sample"]
sample_name = []
output_files = []

with open("nanoaod_inputs.json", "r") as fd:
        data = json.load(fd)
        for sample, conditions in data.items():
                for condition, details in conditions.items():
                        sample_name.append(f"{sample}__{condition}")
                        output_files.append(f"histograms/histograms_{sample}__{condition}.root")

rule all:
    input:
        "histograms.root"

rule process_sample_one_file_in_sample:
    container:
        "registry.cern.ch/docker.io/reanahub/reana-demo-agc-cms-ttbar-coffea:1.0.1"
    resources:
        kerberos=True,
        kubernetes_memory_limit="1850Mi"
    input:
        notebook="ttbar_analysis_reana.ipynb"
    output:
        "histograms/histograms_{sample}__{condition}.root",
        "{sample}__{condition}.png"
    params:
        sample_name = "{sample}__{condition}"
    shell:
        "papermill {input.notebook} $(python prepare_workspace.py sample_{params.sample_name})/sample_{params.sample_name}_out.ipynb "
        "-p sample_name {params.sample_name} -k python3"

rule merging_histograms:
    container:
        "registry.cern.ch/docker.io/reanahub/reana-demo-agc-cms-ttbar-coffea:1.0.1"
    resources:
        kerberos=True,
        kubernetes_memory_limit="8Gi"
    input:
        "nanoaod_inputs.json",
        output_files,
        notebook="final_merging.ipynb"
    output:
        "histograms.root"
    shell:
        "papermill {input.notebook} result_notebook.ipynb -k python3"

    
