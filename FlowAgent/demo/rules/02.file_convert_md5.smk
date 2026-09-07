import csv
import os

SAMPLES = {}
with open(os.path.join(str(workflow.current_basedir), "..", "config", "samples.csv")) as f:
    for row in csv.DictReader(f):
        SAMPLES[row["sample"]] = row

# 软件路径来自 envs/general_software.yaml(由入口/组合 snakefile 以 configfile 加载)
# 生产环境对应 config.software.seq_preprocessor / json_md5_verifier(Flow 规范 §3.3)

rule seq_preprocessor:
    input:
        r1 = expand("00.raw_data/vendor/{sample}/{sample}_1.fq.gz", sample=SAMPLES.keys()),
        r2 = expand("00.raw_data/vendor/{sample}/{sample}_2.fq.gz", sample=SAMPLES.keys()),
    output:
        links_r1 = expand("00.raw_data/link_dir/{sample}/{sample}_R1.fq.gz", sample=SAMPLES.keys()),
        links_r2 = expand("00.raw_data/link_dir/{sample}/{sample}_R2.fq.gz", sample=SAMPLES.keys()),
        json_report = "00.raw_data/link_dir/raw_data_md5.json",
    params:
        binary = config["general_software"]["seq_preprocessor"],
    threads: 1
    log:
        "logs/02.file_convert_md5/seq_preprocessor.log",
    shell:
        "mkdir -p logs/02.file_convert_md5 && "
        "{params.binary} -i $(pwd)/00.raw_data/vendor -o $(pwd)/00.raw_data/link_dir "
        "--library-type short-read --json-report $(pwd)/{output.json_report} "
        "--log-file {log} --log-level info"

rule check_md5:
    input:
        json_report = "00.raw_data/link_dir/raw_data_md5.json",
        links_r1 = expand("00.raw_data/link_dir/{sample}/{sample}_R1.fq.gz", sample=SAMPLES.keys()),
        links_r2 = expand("00.raw_data/link_dir/{sample}/{sample}_R2.fq.gz", sample=SAMPLES.keys()),
    output:
        check_report = "01.qc/md5_check.tsv",
    params:
        binary = config["general_software"]["check_md5"],
    threads: 4
    log:
        "logs/02.file_convert_md5/check_md5.log",
    shell:
        "mkdir -p 01.qc logs/02.file_convert_md5 && "
        "{params.binary} -i {input.json_report} -b 00.raw_data/link_dir "
        "-o {output.check_report} --threads {threads} --log-file {log}"
