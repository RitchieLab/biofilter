Perfeito! Se o `.sh` vai permanecer o mesmo, a única coisa que precisamos garantir é que o ambiente **`biofilter-jupyter` seja criado corretamente** com as versões compatíveis para evitar o erro do `jinja2`.

### ✅ Atualização para o `README.md` (em inglês)

Aqui está uma sugestão de seção para o `README.md`, com foco na criação do ambiente com as versões estáveis:

---

## 🧪 Jupyter Environment Setup

### Step 1: Load Anaconda

```bash
module load anaconda/3
```

### Step 2: Create Conda Environment

Create the Conda environment with compatible versions of Jupyter, nbconvert, and jinja2:

```bash
conda create -n biofilter-jupyter \
    python=3.10 \
    notebook \
    jupyterlab \
    nbconvert=6.5.4 \
    jinja2=3.0.3 \
    ipykernel \
    sqlalchemy -y
```

> ⚠️ The versions of `jinja2` and `nbconvert` are critical to avoid internal server errors when opening notebooks.

### Step 3: Start the Jupyter Server

Use the startup script located in the project root:

```bash
./start_jupyter_conda.sh
```

This script will:

* Activate the Conda environment
* Print Python version and path
* Launch Jupyter Notebook on port `8888` (or next available port)

### Step 4: Access via SSH Tunnel

On your **local machine**, run:

```bash
ssh -N -L 8888:localhost:8888 your_username@superman.pmacs.upenn.edu
```

Then open your browser and go to:

```
http://localhost:8888/?token=<your-token>
```

You’ll find the token printed in the terminal output after Jupyter starts.

---

Se quiser, posso agora gerar o arquivo `README.md` completo com esse conteúdo. Deseja isso?
