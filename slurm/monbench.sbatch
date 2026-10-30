#!/usr/bin/env bash
# Run the two-server D/SOS trial as one batch job on three nodes.
# The first two nodes of the allocation become servers, the third the query
# node (unless SERVER_HOSTS / QUERY_HOST are set in bench.conf or exported
# before sbatch). All nodes but the last become servers; at least 2 are needed.
#
#   sbatch slurm/monbench.sbatch                 # build serve-write write serve-query check query stop
#   sbatch slurm/monbench.sbatch build serve-write write serve-query check live stop
#
#SBATCH --job-name=monbench
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=1
#SBATCH --time=00:30:00
#SBATCH --output=monbench-%j.out

set -euo pipefail
# sbatch runs a copy of this script, so locate the tree from the submit dir.
cd "${SLURM_SUBMIT_DIR}"
PHASES=("$@")
[[ ${#PHASES[@]} -gt 0 ]] || PHASES=(all)
bin/run.sh -l slurm "${PHASES[@]}"

