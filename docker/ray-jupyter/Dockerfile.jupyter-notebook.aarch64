# Use the official aarch64 Ray image as the base
FROM rayproject/ray:2.48.0-py311-aarch64

# Expose the default JupyterLab port
EXPOSE 8888

# Install JupyterLab and create the work directory
WORKDIR /home/ray_user

# Create the specific directory for JupyterLab notebooks
RUN mkdir work

# Install JupyterLab as a separate step
RUN pip install jupyterlab --no-cache-dir

# Set the entrypoint to launch JupyterLab
ENTRYPOINT ["jupyter-lab", "--allow-root", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--notebook-dir=/home/ray_user/work"]
