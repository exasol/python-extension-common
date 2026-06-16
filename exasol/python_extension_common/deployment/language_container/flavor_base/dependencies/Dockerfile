FROM exasol/script-language-container:template-Exasol-all-python-3.10-release_x64_BFOJFIJ2Y42PBIZHWXVEQBBVPAEVHZETG4Z45VAMFWFSXORH3FVA

Run mkdir /project
COPY dependencies/requirements.txt /project/requirements.txt
RUN python3.10 -m pip install -r /project/requirements.txt
