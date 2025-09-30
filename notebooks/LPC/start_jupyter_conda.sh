#!/bin/bash

################################################################################
# Start Jupyter Notebook for Biofilter 3R (Remote Access via SSH Tunnel)
#
# Usage:
#   $ bash start_jupyter_conda.sh
#
# Notes:
# - This script assumes you already have Conda and the `biofilter-jupyter` env.
# - Notebooks should be saved in the /notebooks directory.
#
# ------------------------------------------------------------------------------
# First-time setup (for new users - only once):
#
# Load Anaconda module and create your own environment with required packages:
#   $ module load anaconda/3
#   $ conda create -n biofilter-jupyter python=3.10 jupyter sqlalchemy ipykernel -y
#
# After that, you can simply run:
#   $ bash start_jupyter_conda.sh
# ------------------------------------------------------------------------------
################################################################################

# Load Conda if necessary (adjust based on your cluster setup)
source /appl/anaconda-3/etc/profile.d/conda.sh

# Activate the Conda environment for Jupyter
conda activate biofilter-jupyter

# Navigate to the project root (adjust path if needed)
cd ~/group/software/biofilter3R

# Show Python path and version
echo "Python executable in use:"
which python
python --version
echo ""

echo "Starting Jupyter Notebook..."

# Start Jupyter Notebook without browser, on port 8888
# If port is busy, Jupyter will automatically try 8889, 8890, etc.
jupyter notebook --no-browser --port=8888

