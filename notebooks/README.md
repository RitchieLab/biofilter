# 📘 Using Jupyter with Biofilter 3R on LPC

This guide explains how to launch a Jupyter Notebook server to interactively explore and analyze data using the Biofilter 3R system, directly on the LPC cluster, **without copying the database** to your local machine.

---

## ✅ Requirements

* Active account on LPC (PMACS cluster)
* Biofilter 3R repository cloned in your /group/software directory
* SSH key-based login configured (passwordless strongly recommended)
* Anaconda module available on LPC

---

## 🧪 Step 1 – One-time Setup (Per User)

Load the Anaconda module and create your own isolated Conda environment:

```bash
module load anaconda/3
conda create -n biofilter-jupyter python=3.10 jupyter sqlalchemy ipykernel -y
```

If needed, activate conda in your shell (e.g., for `.bashrc` or `.zshrc`):

```bash
source ~/miniconda3/etc/profile.d/conda.sh  # Or your appropriate path
```

---

## 🚀 Step 2 – Running Jupyter

Inside the `biofilter3R/notebooks/` folder, run the provided script:

```bash
bash start_jupyter_conda.sh
```

This will:

* Activate your Conda environment
* Move to the Biofilter 3R root directory
* Display Python path and version for debugging
* Start Jupyter on port `8888` (or next available if in use)

Example output:

```
Python used:
/appl/anaconda-3/bin/python
Python 3.10.13
Jupyter running at: http://localhost:8888/?token=...
```

⚠️ Copy the full URL with token to access Jupyter from your local browser.

---

## 🖥️ Step 3 – Accessing Jupyter from Your Machine

On your **local machine**, open a terminal and forward the port:

```bash
ssh -N -f -L localhost:8888:localhost:8888 your_lpc_user@superman.pmacs.upenn.edu
```

Then, go to:

```
http://localhost:8888/?token=...
```

Paste the token shown from the previous step.

---

## 📁 Recommended Folder Structure

To keep things organized:

```
biofilter3R/
├── notebooks/               # Notebooks created by users
│   ├── start_jupyter_conda.sh  # Script to start Jupyter
│   └── <your_notebooks>.ipynb
├── biofilter/              # Source code
├── data/                   # Data files (if needed)
└── ...
```

---

## ❓ Troubleshooting

* **Conda not found**: Ensure Anaconda is loaded (`module load anaconda/3`)
* **Token not accepted**: Use the *exact* full URL with token shown in terminal
* **Port already in use**: Jupyter will auto-increment (e.g., 8889, 8890...)
* **No module named X**: Add missing packages using `conda install <package>`

---

## 📬 Need Help?

If you're stuck, contact the system administrator or the Biofilter 3R team.

---

Happy exploring! 🧬
