FROM exasol/script-language-container:template-Exasol-all-python-3.12-release_x64_11.2.0

Run mkdir /project
COPY dependencies/requirements.txt /project/requirements.txt
RUN python3.12 -m pip install --break-system-packages -r /project/requirements.txt
