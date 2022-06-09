#!/bin/bash -l
#
# ++++ THIS IS A CYLC TASK JOB SCRIPT ++++
# Suite: UFSRNR_GSI_SOCA_3DVAR_COUPLED_122015_HC44RS_lstr_tst
# Task: staging_fetch_atmosobs.20151206T0600Z
# Job log directory: 20151206T0600Z/staging_fetch_atmosobs/01
# Job submit method: slurm

# DIRECTIVES:
#SBATCH --job-name=UFSRNR_GSI_SOCA_3DVAR_COUPLED_122015_HC44RS_lstr_tst.staging_fetch_atmosobs.20151206T0600Z
#SBATCH --output=/lustre/home/work/cylc-run/UFSRNR_GSI_SOCA_3DVAR_COUPLED_122015_HC44RS_lstr_tst/log/job/20151206T0600Z/staging_fetch_atmosobs/01/job.out
#SBATCH --error=/lustre/home/work/cylc-run/UFSRNR_GSI_SOCA_3DVAR_COUPLED_122015_HC44RS_lstr_tst/log/job/20151206T0600Z/staging_fetch_atmosobs/01/job.err
#SBATCH --ntasks=1
#SBATCH --partition=compute1
#SBATCH --time=29:00
export CYLC_DIR='/contrib/home/builder/UFS-RNR-stack/cylc-flow/cylc'
export CYLC_VERSION='UNKNOWN'
CYLC_FAIL_SIGNALS='EXIT ERR XCPU'

cylc__job__inst__cylc_env() {
    # CYLC SUITE ENVIRONMENT:
    export CYLC_CYCLING_MODE="gregorian"
    export CYLC_SUITE_FINAL_CYCLE_POINT="20151215T1800Z"
    export CYLC_SUITE_INITIAL_CYCLE_POINT="20151201T0000Z"
    export CYLC_SUITE_NAME="UFSRNR_GSI_SOCA_3DVAR_COUPLED_122015_HC44RS_lstr_tst"
    export CYLC_UTC="True"
    export CYLC_VERBOSE="false"
    export TZ="UTC"

    export CYLC_SUITE_RUN_DIR="/lustre/home/work/cylc-run/UFSRNR_GSI_SOCA_3DVAR_COUPLED_122015_HC44RS_lstr_tst"
    export CYLC_SUITE_DEF_PATH="/lustre/home/work/UFSRNR_GSI_SOCA_3DVAR_COUPLED_122015_HC44RS_lstr_tst/cylc"
    export CYLC_SUITE_DEF_PATH_ON_SUITE_HOST="/lustre/home/work/UFSRNR_GSI_SOCA_3DVAR_COUPLED_122015_HC44RS_lstr_tst/cylc"
    export CYLC_SUITE_UUID="e45da6ee-85dc-4638-9da4-ad96bd60051b"

    # CYLC TASK ENVIRONMENT:
    export CYLC_TASK_JOB="20151206T0600Z/staging_fetch_atmosobs/01"
    export CYLC_TASK_NAMESPACE_HIERARCHY="root staging_fetch_atmosobs"
    export CYLC_TASK_DEPENDENCIES="launch.20151206T0600Z"
    export CYLC_TASK_TRY_NUMBER=1
}

cylc__job__inst__user_env() {
    # TASK RUNTIME ENVIRONMENT:
    export CYCLE EMAILrnr EXPTrnr HOMErnr MAILEVENTSrnr NOSCRUBrnr PLATFORMrnr PRErnr SCHEDULERrnr SINGULARITY_OWNER WORKrnr WORKFLOWrnr YAMLrnr TOTALTASKSrnr
    CYCLE="$(cylc cyclepoint --template=%Y%m%d%H%M%S)"
    EMAILrnr="Steve.Lawrence@noaa.gov"
    EXPTrnr="UFSRNR_GSI_SOCA_3DVAR_COUPLED_122015_HC44RS_lstr_tst"
    HOMErnr="/contrib/home/builder/UFS-RNR-rel-v2.0.0-tag"
    MAILEVENTSrnr="failed"
    NOSCRUBrnr="/lustre/home/noscrub"
    PLATFORMrnr="azure_centos7_hc44rs"
    PRErnr="/contrib/home/builder/UFS-RNR-rel-v2.0.0-tag/modulefiles/LINUX-Centos7.AZURE-PW.UFS-RNR.sh"
    SCHEDULERrnr="slurm"
    SINGULARITY_OWNER="pwuser"
    WORKrnr="/lustre/home/work"
    WORKFLOWrnr="cylc"
    YAMLrnr="/contrib/home/builder/UFS-RNR-rel-v2.0.0-tag/parm/experiments/ufs-rnr.3DVAR.C96L127_1p0.coupled.yaml"
    TOTALTASKSrnr="1"
}

cylc__job__inst__script() {
# SCRIPT:
sh ${HOMErnr}/jobs/JRNR_STAGING_FETCH_ATMOSOBS
}

. "${CYLC_DIR}/lib/cylc/job.sh"
cylc__job__main

#EOF: 20151206T0600Z/staging_fetch_atmosobs/01
