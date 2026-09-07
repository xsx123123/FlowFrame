import csv
import os

SAMPLES = {}
with open(os.path.join(str(workflow.current_basedir), "..", "config", "samples.csv")) as f:
    for row in csv.DictReader(f):
        SAMPLES[row["sample"]] = row

rule short_read_qc_r1:
    input:
        md5_check = "01.qc/md5_check.tsv",
        link_r1_dir = "00.raw_data/link_dir/{sample}/{sample}_R1.fq.gz",
    output:
        r1_html = "01.qc/short_read_qc_r1/{sample}_R1_fastqc.html",
        r1_zip = "01.qc/short_read_qc_r1/{sample}_R1_fastqc.zip",
    threads: 2
    conda:
        "../envs/fastqc.yaml"
    log:
        "logs/03.short_read_qc/fastqc_r1_{sample}.log",
    shell:
        "mkdir -p 01.qc/short_read_qc_r1 logs/03.short_read_qc && "
        "fastqc --threads {threads} --outdir 01.qc/short_read_qc_r1 {input.link_r1_dir} > {log} 2>&1"

rule short_read_qc_r2:
    input:
        md5_check = "01.qc/md5_check.tsv",
        link_r2_dir = "00.raw_data/link_dir/{sample}/{sample}_R2.fq.gz",
    output:
        r2_html = "01.qc/short_read_qc_r2/{sample}_R2_fastqc.html",
        r2_zip = "01.qc/short_read_qc_r2/{sample}_R2_fastqc.zip",
    threads: 2
    conda:
        "../envs/fastqc.yaml"
    log:
        "logs/03.short_read_qc/fastqc_r2_{sample}.log",
    shell:
        "mkdir -p 01.qc/short_read_qc_r2 logs/03.short_read_qc && "
        "fastqc --threads {threads} --outdir 01.qc/short_read_qc_r2 {input.link_r2_dir} > {log} 2>&1"

rule short_read_multiqc_r1:
    input:
        fastqc_files_r1 = expand("01.qc/short_read_qc_r1/{sample}_R1_fastqc.zip", sample=SAMPLES.keys()),
    output:
        report = "01.qc/short_read_r1_multiqc/multiqc_r1_raw-data_report.html",
    threads: 2
    conda:
        "../envs/multiqc.yaml"
    log:
        "logs/03.short_read_qc/multiqc_r1.log",
    shell:
        "mkdir -p 01.qc/short_read_r1_multiqc logs/03.short_read_qc && "
        "multiqc --force --outdir 01.qc/short_read_r1_multiqc "
        "--filename multiqc_r1_raw-data_report.html 01.qc/short_read_qc_r1 > {log} 2>&1"

rule short_read_multiqc_r2:
    input:
        fastqc_files_r2 = expand("01.qc/short_read_qc_r2/{sample}_R2_fastqc.zip", sample=SAMPLES.keys()),
    output:
        report = "01.qc/short_read_r2_multiqc/multiqc_r2_raw-data_report.html",
    threads: 2
    conda:
        "../envs/multiqc.yaml"
    log:
        "logs/03.short_read_qc/multiqc_r2.log",
    shell:
        "mkdir -p 01.qc/short_read_r2_multiqc logs/03.short_read_qc && "
        "multiqc --force --outdir 01.qc/short_read_r2_multiqc "
        "--filename multiqc_r2_raw-data_report.html 01.qc/short_read_qc_r2 > {log} 2>&1"
