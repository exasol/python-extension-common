FROM {{ dependencies }}

COPY release/dist /project/dist
RUN python3.12 -m pip install --no-deps --break-system-packages /project/dist/*.whl

RUN mkdir -p /build_info/actual_installed_packages/release && \
  exaslpm list-all-installed-packages --out-file /build_info/actual_installed_packages/release/packages.yml
