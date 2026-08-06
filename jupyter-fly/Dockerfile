# syntax=docker/dockerfile:1
# Pinned date tag for reproducible Jupyter Docker Stacks builds.
# Update deliberately after smoke testing package and OpenWebUI compatibility.
FROM quay.io/jupyter/scipy-notebook:2025-12-31

USER root

# Install Excel/data-analysis packages that OpenWebUI Code Interpreter users need.
# jupyter/scipy-notebook already includes much of the scientific stack, but these
# packages are installed explicitly so Excel support does not depend on image defaults.
RUN python -m pip install --no-cache-dir \
    pandas \
    numpy \
    openpyxl \
    xlsxwriter \
    matplotlib \
    scipy && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

COPY start-jupyter.sh /usr/local/bin/start-jupyter.sh
RUN chmod 0755 /usr/local/bin/start-jupyter.sh

USER ${NB_UID}
WORKDIR /tmp/jupyter-work

EXPOSE 8888

CMD ["/usr/local/bin/start-jupyter.sh"]
